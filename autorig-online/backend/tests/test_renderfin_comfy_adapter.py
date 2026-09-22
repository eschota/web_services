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


def test_resolve_artifacts_uses_temp_only_when_no_output_exists():
    temp = {"filename": "preview.png", "subfolder": "temp", "type": "temp"}
    output = {"filename": "final.png", "subfolder": "", "type": "output"}

    assert resolve_artifacts(
        {"outputs": {"preview": [temp], "save": [output]}}, output_ext=".png"
    ) == [output]
    assert resolve_artifacts(
        {"outputs": {"preview": [temp]}}, output_ext=".png"
    ) == [temp]
