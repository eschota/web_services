from renderfin.comfy_adapter import resolve_artifacts


def test_resolve_artifacts_ignores_loadvideo_input_and_returns_generated_video():
    history = {
        "outputs": {
            "driving_video": {
                "images": [
                    {
                        "filename": "reference-meeting-25f-24fps.mp4",
                        "subfolder": "",
                        "type": "input",
                    }
                ],
                "animated": [True],
            },
            "save": {
                "images": [
                    {
                        "filename": "task-id_00001_.mp4",
                        "subfolder": "",
                        "type": "output",
                    }
                ],
                "animated": [True],
            },
        }
    }

    artifacts = resolve_artifacts(history, output_ext=".mp4")

    assert artifacts == [
        {"filename": "task-id_00001_.mp4", "subfolder": "", "type": "output"}
    ]


def test_resolve_artifacts_returns_empty_for_input_only_history():
    history = {
        "outputs": {
            "driving_video": {
                "images": [
                    {
                        "filename": "source.mp4",
                        "subfolder": "",
                        "type": "input",
                    }
                ]
            }
        }
    }

    assert resolve_artifacts(history, output_ext=".mp4") == []


def test_resolve_artifacts_never_delivers_temp_previews():
    temp = {"filename": "preview.png", "subfolder": "temp", "type": "temp"}
    output = {"filename": "final.png", "subfolder": "", "type": "output"}

    assert resolve_artifacts(
        {"outputs": {"preview": [temp], "save": [output]}}, output_ext=".png"
    ) == [output]
    assert resolve_artifacts(
        {"outputs": {"preview": [temp]}}, output_ext=".png"
    ) == []


def test_submit_retains_managed_queue_contract():
    import asyncio
    from renderfin.comfy_adapter import submit
    from renderfin.models import RenderServer

    class Response:
        status_code = 200
        def json(self):
            return {"prompt_id": "test-prompt", "node_errors": {}}

    class Client:
        async def post(self, url, **kwargs):
            assert kwargs["json"]["prompt_id"] == "test-prompt"
            assert kwargs["json"]["extra_data"]["autorig_workload"]["logical_task_id"] == "test-task"
            assert kwargs["headers"]["X-AutoRig-Managed-Task-Id"] == "test-task"
            return Response()

    result = asyncio.run(submit(Client(), RenderServer(render_server_name="test",
        render_server_url="http://127.0.0.1:8988"), {},
        managed_identity={"logical_task_id": "test-task", "lease_id": "test-lease", "request_id": "test-request"},
        prompt_id="test-prompt"))
    assert result == "test-prompt"
