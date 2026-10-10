"""Explicit preserve-order mode: one fixed-quality stream, no silent recovery skips."""
from enum import Enum


class DeliveryMode(str,Enum):
    LATEST='latest'
    PRESERVE_ORDER='preserve_order'


class OrderedDecodeGate:
    def __init__(self,epoch,*,max_units=8,gap_timeout_ns=2_000_000_000):
        self.epoch=epoch;self.max_units=max_units;self.gap_timeout_ns=gap_timeout_ns
        self.last_sequence=0;self.last_frame_id=0;self.pending={};self.waiting_idr=False
        self.stats=dict(submitted=0,duplicates_before_decode=0,discarded_for_recovery=0,reference_breaks=0)
    def push(self,unit,now_ns):
        if unit.epoch!=self.epoch:raise ValueError('Wrong ordered epoch')
        if unit.sequence<=self.last_sequence:
            self.stats['duplicates_before_decode']+=1;return []
        if unit.sequence in self.pending:
            if self.pending[unit.sequence][0]!=unit:raise ValueError('Conflicting ordered copy')
            self.stats['duplicates_before_decode']+=1;return []
        if len(self.pending)>=self.max_units:raise RuntimeError('Ordered reorder queue exceeded bound')
        self.pending[unit.sequence]=(unit,now_ns)
        return self.drain(now_ns)
    def drain(self,now_ns):
        ready=[]
        while self.last_sequence+1 in self.pending:
            unit,_=self.pending.pop(self.last_sequence+1)
            if unit.frame_id!=self.last_frame_id+1:raise RuntimeError('Captured frame missing in ordered stream')
            if self.last_sequence==0 and not unit.keyframe:raise RuntimeError('Ordered stream did not begin with keyframe')
            self.last_sequence=unit.sequence;self.last_frame_id=unit.frame_id
            self.stats['submitted']+=1;ready.append(unit)
        if self.pending and now_ns-min(at for _,at in self.pending.values())>=self.gap_timeout_ns:
            raise RuntimeError('Ordered frame gap exceeded bound; no frames skipped')
        return ready
