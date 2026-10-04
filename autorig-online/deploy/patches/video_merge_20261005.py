"""Owner 2026-10-05 (graph 88257c539e2d): «сделай ноду merge, для видео, она должна просто собирать произвольное число
видео инпутов и отображать их как один ролик видео последовательно».

Node «Merge videos» (service video_merge): up to 20 video sockets, each one clip or a list of clips; the clips are
joined in socket order (and list order inside a socket), as rendered (fit native, nothing trimmed), sized to the first.
Backend: POST /api/ai/video-tools/merge flattens the sockets and runs the existing concat job.

    python3 patch_video_merge.py <release root>/autorig-online
Every changed file is unlinked before it is written (releases share files through hardlinks)."""
import pathlib
import sys

ROOT = pathlib.Path(sys.argv[1])
N = 20


def patch(rel, pairs):
    p = ROOT / rel
    s = p.read_text(encoding="utf-8")
    for a, b in pairs:
        assert s.count(a) == 1, (rel, s.count(a), a[:70])
        s = s.replace(a, b)
    if rel.endswith(".py"):
        compile(s, str(p), "exec")
    p.unlink()
    p.write_text(s, encoding="utf-8")
    print("patched", rel)


# 1. backend endpoint
patch("backend/ai_video_tools.py", [(
    '''@router.post("/api/ai/video-tools/audio-mux")''',
    '''class MergeRequest(BaseModel):
    """Merge videos (owner 2026-10-05): video_1..video_20, each a clip URL or a list of them, joined in socket order."""
''' + "".join(f"    video_{i}: Any = None\n" for i in range(1, N + 1)) + '''    out_width: int = Field(0, ge=0, le=4096)
    out_height: int = Field(0, ge=0, le=4096)
    fps: int = Field(24, ge=8, le=60)

    def clips(self) -> List[str]:
        out: List[str] = []
        for i in range(1, ''' + str(N + 1) + '''):
            out += [c for c in _as_list(getattr(self, f"video_{i}")) if str(c or "").strip()]
        return out


@router.post("/api/ai/video-tools/merge")
async def api_merge(body: MergeRequest):
    clips = body.clips()
    if not clips:
        raise HTTPException(status_code=422, detail={"error_string": "nothing_to_merge",
                                                     "message_string": "connect at least one video"})
    concat = ConcatRequest(clip=clips, out_width=body.out_width, out_height=body.out_height, fps=body.fps,
                           trim=False, fit="native")
    return _submit("video_concat", concat, _concat)


@router.post("/api/ai/video-tools/audio-mux")''')])

# 2. the service in the catalogue
patch("backend/ai_services.py", [(
    '''    {
        "id": "audio_from_source", "title": "Audio from source", "path": "/nodes",''',
    '''    {
        # Owner 2026-10-05: any number of video inputs shown as one clip, in socket order.
        "id": "video_merge", "title": "Merge videos", "path": "/nodes",
        "api": "/api/ai/video-tools/merge", "status": "live", "list_sink": True,
        "summary": ("Joins the connected videos one after another into one clip, in socket order (a list input "
                    "keeps its own order), as they were rendered: nothing trimmed, all sized to the first."),
        "inputs": [
''' + "".join(f'            {{"type": VIDEO, "field": "video_{i}", "required": {str(i == 1)}, "title": "Video {i}"}},\n'
              for i in range(1, N + 1)) + '''        ],
        "outputs": [{"type": VIDEO, "field": "video_url_string", "title": "Merged video"}],
    },
    {
        "id": "audio_from_source", "title": "Audio from source", "path": "/nodes",''')])

# 3. the editor: runner, icon, palette, label
patch("static/js/ai-node-lists.js", [(
    '''    video_concat: {api: '/api/ai/video-tools/concat', field: 'video_url_string', type: 'video',''',
    '''    video_merge: {api: '/api/ai/video-tools/merge', field: 'video_url_string', type: 'video',
      finish: async (accepted, runner, report) => (await pollVideoTool(accepted, runner, report)).video_url_string},
    video_concat: {api: '/api/ai/video-tools/concat', field: 'video_url_string', type: 'video',''')])
patch("static/js/ai-nodes.js", [
    ("video_concat: '🔗', audio_from_source: '🔊',", "video_concat: '🔗', video_merge: '🧷', audio_from_source: '🔊',"),
    ("'scene_split', 'video_concat', 'video_summary', 'audio_from_source',",
     "'scene_split', 'video_concat', 'video_merge', 'video_summary', 'audio_from_source',"),
    ("video_concat: 'Concat shots', video_summary: 'Summary',",
     "video_concat: 'Concat shots', video_merge: 'Merge videos', video_summary: 'Summary',"),
])
