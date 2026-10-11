"""Rebuild the public report and derived tables from archived, unedited JSON.

Install reportlab to rebuild the PDF. This does not rerun the benchmark.
"""
import csv
import hashlib
import json
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent
RAW = ROOT/'raw'
capacity = json.loads((RAW/'c548c522-adaptive-video-summary.json').read_text())
outage = json.loads((RAW/'c548c523-adaptive-video-summary.json').read_text())
assert all(d['cleanup_status'] == 'PASS' for d in (capacity, outage))
assert all(t['status'] == 'VALID' for d in (capacity, outage) for t in d['trials'])
phases = ('initial', 'degraded_a', 'all_restored')
candidates = ('adaptive-frame-rate-first', 'srt-high')
values = {}
rows = []
for candidate in candidates:
    trials = [t for t in capacity['freshness_results'] if t['candidate'] == candidate]
    assert len(trials) == 2
    for phase in phases:
        samples = [next(p for p in t['phases'] if p['name'] == phase)['latest_capture_lag_time_mean_ms'] for t in trials]
        values[candidate, phase] = mean(samples)
        for t, value in zip(trials, samples):
            rows.append([capacity['run_id'], candidate, t['repetition'], phase, value])
with (ROOT/'capacity-results.csv').open('w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['run_id', 'candidate', 'repetition', 'phase', 'latest_capture_lag_time_mean_ms'])
    writer.writerows(rows)
improvement = 100*(1-values[candidates[0], phases[1]]/values[candidates[1], phases[1]])
assert round(improvement, 2) == 78.85
assert round(values[candidates[0], phases[1]], 2) == 33.53
assert round(values[candidates[1], phases[1]], 2) == 158.56
metrics = {t['candidate']: t['metrics'] for t in outage['trials']}
assert round(metrics['adaptive']['fresh_gap_max_ms'], 2) == 87.75
assert metrics['adaptive']['freeze_count'] == 0
hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(RAW.glob('*.json'))}
manifest = {'report_date': '2026-10-11', 'experiment_date': '2026-10-08',
    'scope': 'Historical lab measurements; not a rerun of current GUI or current SDK.',
    'files': hashes,
    'archive_sha256': 'dadd1598a330905db98c2359ffeb2fe1ece12647e0702ce2262ecc7794ac919e',
    'provenance': 'Four unedited summary files extracted from video-checkpoint-c548c522-c548c523-20261008.tar.gz. Archive and complete per-frame logs are not included.'}
(ROOT/'evidence-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
phase_table = [[label, f'{values[candidates[0], phase]:.2f}', f'{values[candidates[1], phase]:.2f}'] for label, phase in zip(('Initial', 'Reduced capacity', 'Restored'), phases)]
outage_table = []
for label, key, decimals in [('New decoded frames', 'decoded_frames', 0), ('New-frame FPS', 'new_frame_fps', 2), ('Frame age median (ms)', 'age_median_ms', 2), ('Frame age p95 (ms)', 'age_p95_ms', 2), ('Largest fresh-update gap (ms)', 'fresh_gap_max_ms', 2), ('Gaps above 100 ms', 'freeze_count', 0)]:
    outage_table.append([label, f'{metrics["adaptive"][key]:.{decimals}f}', f'{metrics["srt-high"][key]:.{decimals}f}'])

sections = [
('Executive summary', [
    'ShenLink is a reusable multipath communications foundation for video, commands and sensors. This report publishes historical video measurements against a specific native SRT Broadcast configuration, together with the underlying archived summaries.',
    f'During reduced capacity, mean latest-capture lag was {values[candidates[0], phases[1]]:.2f} ms for ShenLink versus {values[candidates[1], phases[1]]:.2f} ms for SRT: {improvement:.2f}% lower across two repetitions per compared variant.',
    'A separate individual-path outage run recorded a largest fresh-update gap of 87.75 ms for ShenLink and zero gaps above 100 ms. These observations are specific to the measured lab runs, not a promise of zero latency, zero packet loss or uninterrupted operation under all failures.',
    'Experiments: 8 October 2026. Report compiled: 11 October 2026. Run IDs: c548c522 and c548c523. No new performance experiment was conducted for this report.']),
('Method and measurement boundaries', [
    'Platform: Linux aarch64, kernel 6.8.0-1065-raspi. Three routed namespace paths shaped with Linux kernel tools independently of the SDK; not three physical radios. This report does not establish a two-physical-computer video comparison.',
    'Source: synthetic moving zone plate, 720p at 30 FPS, same source for compared candidates. Native H.264 low-latency processing. Path A/B/C uplinks: 6/4/1.4 Mbps; RTT: 10/20/60 ms; jitter: 1/2/5 ms. These are historical scenario profiles, not current demo settings or military-network averages.',
    'ShenLink: public video endpoints, dynamic frame-rate-first policy, negotiated 24-30 FPS and 360p-720p range. SRT: native 1.5.3 Broadcast group, three connected members, fixed 720p at 1800 kbps, configured latency buffer 120 ms and peer idle 60 s.',
    'Latest-capture lag is the time-weighted timestamp difference between the newest captured source and the freshest application-decoded frame; silence is included. It is not RTT, exact path-switch time, command-response time or camera-to-screen latency.',
    'Fresh-update gap is elapsed time without a newer public decoded frame. Frame age and latest-capture lag are different metrics. Quality upgrades/duplicates are not additional captured frames. No image-fidelity score was collected in these runs.']),
('Capacity reduction - c548c522', [
    'Path A was reduced from 6 to 1.3 Mbps, then restored. Nominal steady windows: initial 6 s, degraded 8 s, restored 6 s, plus verified transition intervals. Two trials per variant; comparison order was reversed in the second block. The full raw run includes four variants and eight VALID trials; the headline compares frame-rate-first with SRT-high.',
    'The table reports the arithmetic mean of the two per-trial time-weighted latest-capture lag values, in milliseconds. Values are not pooled across every timestamp or across phases.',
    'Reduced-capacity repetitions: ShenLink 33.1524 and 33.9121 ms; SRT 158.3920 and 158.7263 ms. Calculation: 100 x (1 - mean(ShenLink) / mean(SRT)) = 78.8519%.',
    'SRT restored-window lag varied from 177.3972 to 1409.5897 ms. Show this variation with the 793.49 ms mean; do not use that mean as a general recovery-time claim. ShenLink adapts quality/FPS while SRT stays fixed at 720p. The improvement is not demonstrated at identical visual quality or against optimally tuned SRT.']),
('Individual outages - c548c523', [
    'One trial per candidate. Cut A, then C, then B, one at a time for 6 s, restoring for 10 s between cuts; two alternatives remain available. Cuts are verified 100% bidirectional router packet loss, not radio carrier failure. Whole-trial measurement is approximately 62 s, including mutation/readback intervals.',
    'ShenLink continued producing fresh frames during the three individual outages. Public video was available in its final restored window. The tested SRT pipeline did not produce public video in its final restored window.',
    'The SRT pipeline stalled for a long interval; the cause remains unresolved and could involve the bridge, decoder or configuration. The later B outage is confounded by that earlier stall and is not an independent comparison. Connected transport members do not prove decoded-video recovery. CPU during that stall is not a fair efficiency comparison.']),
('Evidence, version mapping and limitations', [
    'The raw folder contains four unedited archived JSON summaries, including counters, event timing brackets, implementation hashes and measured results. It is not the complete archive, packet capture collection or every per-frame endpoint log. Derived capacity-results.csv permits checking the headline from individual repetitions.',
    'Raw report SHA-256 values are listed in evidence-manifest.json. The source archive checksum was previously verified. Its filename is video-checkpoint-c548c522-c548c523-20261008.tar.gz; it is not bundled here.',
    'These results belong to the implementation hashes recorded in the historical runs. Several measured modules match the later standalone SDK; others, including dual.py, changed afterwards. The current ShenLink 0.1.0rc1 and mission GUI have not been freshly benchmarked against SRT by this report.',
    'Small sample sizes, different buffering/quality policies, unmeasured perceptual quality, unresolved SRT stalls and lab-only networking limit conclusions. No field roaming, security certification, physical drone safety, zero packet loss, universal protocol superiority or guarantee through total disconnection is established.',
    'The current mission GUI uses different uplink profiles (10/6/4 Mbps) and a recorded 360-degree source. Its commands/sensors and automatic cuts are a separate demonstration; do not attach these historical video numbers to that scenario as newly measured outcomes.',
    'Rebuild: install reportlab, then run python reports/build_report.py from the SDK checkout. This recalculates derived values and formats the report; it does not rerun SRT. Historical runner/native bridge dependencies are not packaged here, so full benchmark reproduction is not a one-command capability of this SDK repository.'])]

md = ['# ShenLink test reports: video freshness and path outages', '', '[Download PDF](ShenLink-test-report.pdf) | [Raw evidence](raw/) | [Derived CSV](capacity-results.csv) | [Checksums](evidence-manifest.json)', '']
for title, paragraphs in sections:
    md += ['## '+title, '']
    for para in paragraphs:
        md += [para, '']
    table = phase_table if title.startswith('Capacity') else outage_table if title.startswith('Individual') else None
    if table:
        md += ['| Metric / phase | ShenLink | Tested SRT |', '| --- | ---: | ---: |']
        md += ['| '+' | '.join(row)+' |' for row in table]
        md += ['']
md += ['## Raw file SHA-256', '']
md += [f'- `{name}`: `{digest}`' for name, digest in hashes.items()]
(ROOT/'README.md').write_text('\n'.join(md)+'\n', encoding='utf-8')

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from xml.sax.saxutils import escape
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='BodyReport', fontName='Helvetica', fontSize=10, leading=15, textColor=colors.HexColor('#263d42'), spaceAfter=12))
styles['Heading1'].textColor = colors.HexColor('#087d68')
styles['Heading1'].fontSize = 21
styles['Heading1'].leading = 26

def paragraph(text):
    return Paragraph(escape(text), styles['BodyReport'])

def table(data):
    obj = Table([[paragraph(x) for x in row] for row in data], colWidths=[235, 120, 120], repeatRows=1)
    obj.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor('#e7f2ed')), ('VALIGN', (0,0), (-1,-1), 'TOP'), ('LINEBELOW', (0,0), (-1,-1), .4, colors.HexColor('#cedbd7')), ('TOPPADDING', (0,0), (-1,-1), 10), ('BOTTOMPADDING', (0,0), (-1,-1), 6)]))
    return obj

story = []
for index, (title, paras) in enumerate(sections):
    if index:
        story.append(PageBreak())
    else:
        story += [paragraph('SHENLINK / LAB EVIDENCE / 11 OCTOBER 2026'), Spacer(1,12)]
    story.append(Paragraph(title, styles['Heading1']))
    story.append(Spacer(1,14))
    if index == 0:
        story += [table([['Reduced-capacity freshness lag', 'ShenLink', 'Tested SRT'], ['Two-repetition mean', '33.53 ms', '158.56 ms'], ['Relative reduction', '78.85%', 'Reference']]), Spacer(1,18)]
    for para in paras:
        story.append(paragraph(para))
    if title.startswith('Capacity'):
        story.append(table([['Latest-capture lag (ms)', 'ShenLink', 'Tested SRT'], *phase_table]))
    if title.startswith('Individual'):
        story.append(table([['Whole-trial metric', 'ShenLink', 'Tested SRT'], *outage_table]))
    if index == len(sections)-1:
        story.append(paragraph('Evidence: github.com/MahmoudMoubarez/ShenLink-v1/tree/main/reports'))

def footer(canvas, doc):
    canvas.setStrokeColor(colors.HexColor('#cedbd7'))
    canvas.line(48,43,A4[0]-48,43)
    canvas.setFont('Helvetica',8)
    canvas.setFillColor(colors.HexColor('#5b7379'))
    canvas.drawString(48,29,'ShenLink | Historical lab results | Not field validation')
    canvas.drawRightString(A4[0]-48,29,str(doc.page))

SimpleDocTemplate(str(ROOT/'ShenLink-test-report.pdf'), pagesize=A4, rightMargin=48, leftMargin=48, topMargin=48, bottomMargin=60, title='ShenLink video test report', author='ShenLink').build(story, onFirstPage=footer, onLaterPages=footer)
print(f'Report built; recalculated reduction {improvement:.6f}%; {len(hashes)} raw files verified.')
