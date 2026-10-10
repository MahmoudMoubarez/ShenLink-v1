"""Public latest-frame endpoints: receiver negotiation and codec lifecycle."""
from dataclasses import asdict,replace
from collections import deque
import json
import math
import queue
import secrets
import threading
import time
from .configuration import SenderSettings,ReceiverSettings
from .shared.adaptive import AdaptiveConfig,AdaptiveVideoSender,AdaptiveVideoReceiver
from .shared.ladder import quality_ladder
from .shared.fps import adaptation_ladder
from .shared.negotiation import ReceiverRequirements,NegotiationError
from .shared.transport_path import TransportHub,MessageVideoPath
from .shared.ordered import DeliveryMode
from .shared.session import SharedVideoSender,SharedVideoReceiver


def agreed_config(request,source):
    """Intersect receiver bounds with raw source abilities, without going below min."""
    if not isinstance(source,dict) or set(source) not in ({"width","height","fps"},
                                                       {"width","height","fps","min_fps","max_fps"}):
        raise ValueError("Invalid source capabilities")
    if any(type(v) is not int or v<=0 for v in source.values()):raise ValueError("Invalid source capabilities")
    required={"requirements","quality_steps","delay_policy"}
    optional={'delivery_mode','dynamic_fps','fps_steps','adaptation_priority'}
    if not isinstance(request,dict) or not required<=set(request) or set(request)-required-optional:
        raise ValueError("Invalid receiver request")
    requirements=ReceiverRequirements(**request["requirements"])
    dynamic=request.get('dynamic_fps',False)
    source_min=source.get('min_fps',1);source_max=source.get('max_fps',source['fps'])
    if not source_min<=source_max<=source['fps']:raise ValueError('Invalid sender FPS range')
    low=max(requirements.min_fps,source_min);high=min(requirements.max_fps,source_max)
    if low>high:raise ValueError('No common FPS range')
    if not dynamic and not low<=source['fps']<=high:
        raise ValueError('Fixed capture cadence is outside common FPS range; enable dynamic_fps')
    if dynamic:requirements=replace(requirements,min_fps=low,max_fps=high)
    supported=replace(requirements,max_resolution=(
        min(requirements.max_resolution[0],source["width"]),
        min(requirements.max_resolution[1],source["height"])))
    levels=(adaptation_ladder(supported,request['quality_steps'],request.get('fps_steps',3),
            request.get('adaptation_priority','frame_rate_first')) if dynamic else
            quality_ladder(supported,request["quality_steps"],fps=source["fps"]))
    config=AdaptiveConfig(requirements=requirements,levels=levels,
        quality_steps=request["quality_steps"],delay_policy=request["delay_policy"],
        delivery_mode=request.get("delivery_mode",DeliveryMode.LATEST),dynamic_fps=dynamic,
        fps_steps=request.get('fps_steps',3),adaptation_priority=request.get('adaptation_priority','frame_rate_first'))
    if config.delivery_mode==DeliveryMode.PRESERVE_ORDER and config.delay_policy.value!='best_available':
        raise ValueError('Preserve-order cannot use an age-dropping policy')
    if config.delivery_mode==DeliveryMode.LATEST:config.dual("validation")
    return config


class _Endpoint:
    def __init__(self,settings,role,*,on_error=None,on_state=None,on_session=None,
                 auto_recover=True,timeout_s=12.):
        if not math.isfinite(timeout_s) or not 0<timeout_s<=60:raise ValueError("Invalid negotiation timeout")
        if type(auto_recover) is not bool:raise ValueError('auto_recover must be boolean')
        if on_session is not None and not callable(on_session):raise TypeError('on_session must be callable')
        self.settings,self.role=settings,role
        self.on_error,self.on_state=on_error,on_state
        self.on_session,self.auto_recover=on_session,auto_recover
        self.session_generation=0
        self._recovery_deadline=None
        self._retired_ids=deque(maxlen=32)
        self._retired_epochs=deque(maxlen=32)
        self.timeout_s=timeout_s
        self.state="NEW"
        self.error=None
        self.config=None
        self.epoch=None
        self._engine=None
        self._stop=threading.Event()
        self._agreed=threading.Event()
        self._media_ready=threading.Event()
        self._media_message=None
        self._last_ordered_frame=0
        self._ordered_end=None
        self._ordered_done=threading.Event()
        self._expected_end=None
        self._control=queue.Queue(maxsize=64)
        self._thread=None
        self._outgoing=None
        self._rid=secrets.token_hex(12) if role=="receiver" else None
        self._hubs=[TransportHub(p,self._enqueue,self._fail,self._transport_state) for p in settings.paths]
        self._request=None
        self._source=None
        self._close_lock=threading.Lock()
        self._lifecycle_lock=threading.Lock()
        self._cleanup_thread=None
        self.cleanup_errors=[]

    def _transport_state(self,path_id,state,reason):
        if self.on_state:self.on_state(path_id,state,reason)
        if (self.state=='STREAMING' and self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER
                and state.value=='failed'
                and all(h.adapter.state.value in ('failed','closed') for h in self._hubs)):
            self._fail(ConnectionError('All ordered transports failed; completeness not guaranteed'))

    def _enqueue(self,value):
        try:self._control.put_nowait(value)
        except queue.Full:pass

    def _fail(self,error):
        if self.error is None:
            self.error=error
            self.state="FAILED"
            self._stop.set()
            if self._engine is not None:
                self._cleanup_thread=threading.Thread(target=self._background_stop,name="video-error-cleanup",daemon=True)
                self._cleanup_thread.start()
            if self.on_error:
                try:self.on_error(error)
                except Exception:pass

    def _background_stop(self):
        try:self.stop()
        except Exception as exc:self.cleanup_errors.append(str(exc))

    def _message(self,phase,**fields):
        return dict(version=1,id=self._rid,phase=phase,**fields)

    def _can_recover(self):
        return (self.auto_recover and self.state=='STREAMING' and self.config is not None
                and self.config.delivery_mode==DeliveryMode.LATEST)

    def _session_event(self,status,**fields):
        if self.on_session:self.on_session(dict(status=status,epoch=self.epoch,
            generation=self.session_generation,**fields))

    def _begin_recovery(self,reason):
        """Quiesce the old engine, retain adapter ownership, negotiate a new epoch."""
        if not self._can_recover():raise NegotiationError('Peer restarted; current mode requires new endpoints')
        with self._lifecycle_lock:
            if self._stop.is_set():raise RuntimeError('Recovery cancelled')
            old=self.epoch;self.state='RECOVERING'
            self._recovery_deadline=time.monotonic()+self.timeout_s
            if self._engine:self._engine.stop()
            self._engine=None
            for hub in self._hubs:hub.channels.clear()
            self._retired_epochs.append(old)
            self._session_event('interrupted',reason=reason,frames_during_gap_not_replayed=True)
            self.config=self.epoch=None
            self._agreed.clear();self._media_ready.clear();self._media_message=None
            self._outgoing=None

    def _restart_engine(self):
        with self._lifecycle_lock:
            if self._stop.is_set():return
            self._engine=self._build_engine()
            self.session_generation+=1
            self._session_event('starting',reason='peer_restart')
            self._engine.start()
            self.state='STREAMING';self._recovery_deadline=None

    def _receive(self,value):
        if not isinstance(value,dict) or value.get("version")!=1:return
        rid=value.get("id")
        if not isinstance(rid,str) or len(rid)!=24 or any(c not in "0123456789abcdef" for c in rid):return
        phase=value.get("phase")
        if self.role=="sender" and phase=="HELLO":
            if rid in self._retired_ids:return
            if self._rid is not None:
                if rid==self._rid:return
                if self.state not in ('STREAMING','FINISHED'):return
                # Validate the new peer request before stopping a good stream.
                source=self._source_capabilities()
                candidate=agreed_config(value.get('request'),source)
                if value.get('paths')!=sorted(p.path_id for p in self.settings.paths):return
                if candidate.delivery_mode!=DeliveryMode.LATEST:
                    raise NegotiationError('Switching into ordered mode requires new endpoints')
                previous=self._rid
                self._begin_recovery('receiver_restart')
                self._retired_ids.append(previous)
                self._rid=None
            self._rid=rid
            self._request=value.get("request")
            try:
                if value.get("paths")!=sorted(p.path_id for p in self.settings.paths):
                    raise ValueError("Sender and receiver path IDs differ")
                self._source=self._source_capabilities()
                self.config=agreed_config(self._request,self._source)
                if self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER and not all(
                        p.capabilities.reliable and p.capabilities.ordered for p in self.settings.paths):
                    raise ValueError('Sender preserve-order paths must be reliable and ordered')
                self.epoch=secrets.token_hex(16)
                self._outgoing=self._message("OFFER",source=self._source,epoch=self.epoch,
                    contract=self.config.contract_id())
            except (ValueError,TypeError) as exc:
                self._outgoing=self._message("REJECT",reason=str(exc)[:200])
            return
        if rid!=self._rid:return
        if phase=="REJECT":
            raise NegotiationError("No common video configuration: "+str(value.get("reason"))[:200])
        if self.role=="receiver" and phase=="OFFER":
            config=agreed_config(self._request,value.get("source"))
            epoch=value.get("epoch")
            if not isinstance(epoch,str) or len(epoch)!=32 or any(c not in "0123456789abcdef" for c in epoch):
                raise NegotiationError("Invalid video epoch")
            if config.contract_id()!=value.get("contract"):raise NegotiationError("Configuration mismatch")
            if epoch in self._retired_epochs:return
            if self.config is not None and (self.config!=config or self.epoch!=epoch):
                if config.delivery_mode!=DeliveryMode.LATEST:raise NegotiationError('Ordered session interrupted')
                self._begin_recovery('sender_restart')
            self.config,self.epoch=config,epoch
            self._outgoing=self._message("ACCEPT",contract=config.contract_id(),epoch=epoch)
            return
        if self.config is None or value.get("contract")!=self.config.contract_id() or value.get("epoch")!=self.epoch:return
        if self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            if self.role=='receiver' and phase=='ORDERED_END':
                last=value.get('last_frame_id')
                if type(last) is not int or last<0:raise ValueError('Invalid ordered end marker')
                if self._ordered_end is not None and self._ordered_end!=last:raise ValueError('Conflicting ordered end')
                self._ordered_end=last
                self._ack_ordered_end()
                return
            if (self.role=='sender' and phase=='ORDERED_DONE' and self._expected_end is not None
                    and type(value.get('last_frame_id')) is int and value['last_frame_id']==self._expected_end):
                self._ordered_done.set()
                return
        if self.role=="sender" and phase=="MEDIA_READY":
            self._media_ready.set()
            for hub in self._hubs:hub.control(self._message("MEDIA_ACK",contract=self.config.contract_id(),epoch=self.epoch))
            return
        if self.role=="receiver" and phase=="MEDIA_ACK":
            self._media_message=None
            return
        if self.role=="sender" and phase=="ACCEPT":
            self._outgoing=self._message("COMMIT",contract=self.config.contract_id(),epoch=self.epoch)
        elif self.role=="receiver" and phase=="COMMIT":
            self._outgoing=self._message("READY",contract=self.config.contract_id(),epoch=self.epoch)
            for hub in self._hubs:hub.control(self._outgoing)
            self._agreed.set()
        elif self.role=="sender" and phase=="READY":
            self._outgoing=None
            self._agreed.set()

    def _supervise(self):
        last=0.;last_hello=0.
        try:
            while not self._stop.is_set():
                try:
                    value=self._control.get(timeout=.01)
                    self._receive(value)
                except queue.Empty:pass
                now=time.monotonic()
                if self.state=='RECOVERING':
                    if now>=self._recovery_deadline:raise NegotiationError('Peer restart negotiation timed out')
                    if self._agreed.is_set():self._restart_engine()
                if now-last>=.2:
                    last=now
                    for hub in self._hubs:
                        hub.maintain()
                        if not self._agreed.is_set() and self._outgoing:hub.control(self._outgoing)
                        if self._media_message:hub.control(self._media_message)
                    # A restarted sender has no knowledge of the running receiver.
                    # The same-id HELLO is a no-op on a healthy sender, not a new
                    # negotiation or media probe. No additional polling thread.
                    if (self.role=='receiver' and self.state=='STREAMING'
                            and self.config.delivery_mode==DeliveryMode.LATEST and now-last_hello>=1.):
                        last_hello=now
                        hello=self._message('HELLO',request=self._request,
                            paths=sorted(p.path_id for p in self.settings.paths))
                        for hub in self._hubs:hub.control(hello)
                    if (self.state=='STREAMING' and self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER
                            and all(h.adapter.state.value in ('failed','closed') for h in self._hubs)):
                        raise ConnectionError('All ordered transports unavailable; stream completeness not guaranteed')
        except Exception as exc:self._fail(exc)

    def start(self):
        """Wait for negotiation (bounded by timeout_s), then start native codecs."""
        with self._lifecycle_lock:
            if self.state!="NEW":raise RuntimeError("Endpoint is single-use; construct a new endpoint after stop")
            self.state="STARTING"
        try:
            if self.role=="receiver":
                self._request=dict(requirements=asdict(self.settings.requirements),
                    quality_steps=self.settings.quality_steps,delay_policy=self.settings.delay_policy.value,
                    delivery_mode=self.settings.delivery_mode.value)
                self._request.update(dynamic_fps=self.settings.dynamic_fps,fps_steps=self.settings.fps_steps,
                                     adaptation_priority=self.settings.adaptation_priority.value)
                self._outgoing=self._message("HELLO",request=self._request,
                    paths=sorted(p.path_id for p in self.settings.paths))
            for hub in self._hubs:hub.open()
            self._thread=threading.Thread(target=self._supervise,name="video-endpoint-control",daemon=True)
            self._thread.start()
            deadline=time.monotonic()+self.timeout_s
            while not self._agreed.wait(.01):
                if self._stop.is_set():raise self.error or RuntimeError("Startup cancelled")
                if time.monotonic()>=deadline:
                    reason=self._outgoing.get("reason") if self._outgoing and self._outgoing.get("phase")=="REJECT" else "Peer did not complete video negotiation"
                    raise NegotiationError(reason)
            with self._lifecycle_lock:
                if self._stop.is_set():raise self.error or RuntimeError("Startup cancelled")
                self._engine=self._build_engine()
                if self.role=="sender" and self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
                    while not self._media_ready.wait(.01):
                        if self._stop.is_set():raise self.error or RuntimeError('Startup cancelled')
                        if time.monotonic()>=deadline:raise NegotiationError('Ordered receiver media readiness timed out')
                self.session_generation+=1
                self._session_event('starting',reason='initial')
                self._engine.start()
                if self.role=="receiver" and self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
                    self._media_message=self._message("MEDIA_READY",contract=self.config.contract_id(),epoch=self.epoch)
                    for hub in self._hubs:hub.control(self._media_message)
                self.state="STREAMING"
            return self
        except Exception as exc:
            self._fail(exc)
            self.stop()
            raise

    def _paths(self):
        base=[MessageVideoPath(h,"baseline",self.epoch+"-base") for h in self._hubs]
        high=[MessageVideoPath(h,"high",self.epoch+"-high") for h in self._hubs]
        return base,high

    def _ack_ordered_end(self):
        if self._ordered_end is not None and self._last_ordered_frame==self._ordered_end:
            self.state='FINISHED'
            if self._engine is not None:self._engine.ordered_finished=True
            value=self._message('ORDERED_DONE',contract=self.config.contract_id(),epoch=self.epoch,
                                last_frame_id=self._ordered_end)
            for hub in self._hubs:hub.control(value)

    def stop(self):
        self._stop.set()
        errors=[]
        with self._close_lock,self._lifecycle_lock:
            try:
                if self._engine:self._engine.stop()
            except Exception as exc:errors.append(exc)
            finally:
                for hub in self._hubs:
                    try:hub.close()
                    except Exception as exc:errors.append(exc)
                if self.state!="FAILED":self.state="STOPPED"
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
            if self._thread.is_alive():errors.append(RuntimeError("Endpoint supervisor did not stop"))
        if errors:
            self.state="FAILED"
            raise errors[0]

    def counters(self):
        return dict(state=self.state,epoch=self.epoch,cleanup_errors=list(self.cleanup_errors),
            session_generation=self.session_generation,
            agreed_config=asdict(self.config) if self.config else None,
            video=self._engine.counters() if self._engine else None)

    def __enter__(self):return self.start()
    def __exit__(self,*args):self.stop()


class VideoSender(_Endpoint):
    def __init__(self,settings:SenderSettings,*,on_source=None,on_capture=None,**kwargs):
        if not isinstance(settings,SenderSettings):raise TypeError("SenderSettings required")
        self.on_source,self.on_capture=on_source,on_capture
        super().__init__(settings,"sender",**kwargs)

    def _source_capabilities(self):
        return dict(width=self.settings.source.width,height=self.settings.source.height,
                    fps=self.settings.source.fps,min_fps=self.settings.min_fps,max_fps=self.settings.max_fps)

    def _build_engine(self):
        if self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            paths=[MessageVideoPath(h,"baseline",self.epoch+"-base") for h in self._hubs]
            return SharedVideoSender(self.settings.source,paths,self.epoch+"-base",self.config.levels[-1],
                preserve_order=True,on_source=self.on_source,on_error=self._fail)
        base,high=self._paths()
        return AdaptiveVideoSender(self.settings.source,base,high,self.epoch,self.config,
            on_source=self.on_source,on_capture=self.on_capture,on_error=self._fail)

    def finish(self,timeout_s=5.):
        """Ordered-only: drain codecs and confirm final application delivery.

        stop() remains cancellation. finish() never promises completion after
        total disconnection: timeout/failure is explicit, and the endpoint must
        still be stopped by its owner/context manager.
        """
        if not math.isfinite(timeout_s) or not 0<timeout_s<=60:raise ValueError('Invalid finish timeout')
        if self.state!='STREAMING' or self.config.delivery_mode!=DeliveryMode.PRESERVE_ORDER:
            raise RuntimeError('finish requires a streaming preserve-order sender')
        from .shared.session import gst_modules
        Gst=gst_modules();deadline=time.monotonic()+timeout_s
        try:
            if not self._engine.pipeline.send_event(Gst.Event.new_eos()):raise RuntimeError('Ordered EOS refused')
            message=self._engine.pipeline.get_bus().timed_pop_filtered(
                int(timeout_s*Gst.SECOND),Gst.MessageType.EOS|Gst.MessageType.ERROR)
            if message is None:raise TimeoutError('Ordered codec drain timed out')
            if message.type==Gst.MessageType.ERROR:raise RuntimeError(str(message.parse_error()))
            self._expected_end=self._engine.stats['captured']
            marker=self._message('ORDERED_END',contract=self.config.contract_id(),epoch=self.epoch,
                                 last_frame_id=self._expected_end)
            while not self._ordered_done.is_set():
                if self._stop.is_set():raise self.error or RuntimeError('Ordered finish cancelled')
                if time.monotonic()>=deadline:raise TimeoutError('Final ordered application delivery not confirmed')
                for hub in self._hubs:hub.control(marker)
                self._ordered_done.wait(min(.1,max(0,deadline-time.monotonic())))
            self.state='FINISHED'
            return dict(last_frame_id=self._expected_end,application_delivery_confirmed=True)
        except Exception as exc:
            self._fail(exc)
            raise


class VideoReceiver(_Endpoint):
    def __init__(self,settings:ReceiverSettings,*,on_frame=None,on_delivery_status=None,**kwargs):
        if not isinstance(settings,ReceiverSettings):raise TypeError("ReceiverSettings required")
        self.on_frame,self.on_delivery_status=on_frame,on_delivery_status
        super().__init__(settings,"receiver",**kwargs)

    def _build_engine(self):
        if self.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            paths=[MessageVideoPath(h,"baseline",self.epoch+"-base") for h in self._hubs]
            return SharedVideoReceiver(paths,self.epoch+"-base",self.config.levels[-1],
                preserve_order=True,on_frame=self._ordered_frame,on_error=self._fail,
                delay_policy=self.config.delay_policy)
        base,high=self._paths()
        return AdaptiveVideoReceiver(base,high,self.epoch,self.config,
            on_frame=lambda tier,frame:self._latest_frame(frame),
            on_delivery_status=self.on_delivery_status,on_error=self._fail)

    def _latest_frame(self,frame):
        # Both codec tiers belong to one application session. Frame IDs restart
        # only with a new session_id; do not rewrite the clock-mapping epoch.
        if self.on_frame:self.on_frame(replace(frame,session_id=self.epoch))

    def _ordered_frame(self,frame):
        if self.on_delivery_status:
            age=frame.timing.age_upper_ms if frame.timing else None
            self.on_delivery_status(dict(path=frame.path_id,tier='ordered',frame_id=frame.frame_id,
                age_upper_ms=age,max_frame_age_ms=self.config.requirements.max_frame_age_ms,
                within_delay_budget=None if age is None else age<=self.config.requirements.max_frame_age_ms,
                delay_policy='best_available'))
        if self.on_frame:self.on_frame(frame)
        self._last_ordered_frame=frame.frame_id
        self._ack_ordered_end()
