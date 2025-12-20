# AutoRig Online API Examples

This repository contains examples and documentation for integrating with the AutoRig Online API for automated 3D character rigging.

## Features

- **Automated Character Rigging**: AI-powered bone generation and skinning
- **Multiple Formats**: Support for GLB, FBX, and OBJ files
- **50+ Animations**: Professional animation sets included
- **Game Engine Ready**: Unity Mecanim and Unreal Engine compatible

## API Endpoints

### Upload and Rig Character

```bash
curl -X POST "https://autorig.online/api/task/create" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@character.glb" \
  -F "format=glb"
```

### Check Task Status

```python
import requests

task_id = "your-task-id"
response = requests.get(f"https://autorig.online/api/task/{task_id}")
print(response.json())
```

### Download Rigged Result

```python
import requests

task_id = "your-task-id"
response = requests.get(f"https://autorig.online/api/task/{task_id}/download")

with open("rigged_character.glb", "wb") as f:
    f.write(response.content)
```

## Python Example

```python
import requests

class AutoRigClient:
    def __init__(self, api_url="https://autorig.online"):
        self.api_url = api_url

    def upload_and_rig(self, file_path, format="glb"):
        """Upload a 3D model and get it rigged automatically"""

        with open(file_path, "rb") as f:
            files = {"file": f}
            data = {"format": format}

            response = requests.post(
                f"{self.api_url}/api/task/create",
                files=files,
                data=data
            )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API Error: {response.status_code}")

    def get_task_status(self, task_id):
        """Check rigging task status"""

        response = requests.get(f"{self.api_url}/api/task/{task_id}")

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Task not found: {task_id}")

    def download_result(self, task_id, output_path):
        """Download the rigged character"""

        response = requests.get(f"{self.api_url}/api/task/{task_id}/download")

        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            return True
        else:
            raise Exception(f"Download failed: {response.status_code}")

# Usage example
if __name__ == "__main__":
    client = AutoRigClient()

    # Upload and start rigging
    result = client.upload_and_rig("character.glb")
    task_id = result["task_id"]
    print(f"Started rigging task: {task_id}")

    # Wait for completion (in real code, add proper polling)
    import time
    time.sleep(60)  # Wait 1 minute

    # Check status
    status = client.get_task_status(task_id)
    print(f"Task status: {status['status']}")

    # Download result
    if status["status"] == "completed":
        client.download_result(task_id, "rigged_character.glb")
        print("Downloaded rigged character!")
```

## JavaScript/Node.js Example

```javascript
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');

class AutoRigAPI {
    constructor(baseURL = 'https://autorig.online') {
        this.baseURL = baseURL;
    }

    async uploadModel(filePath, format = 'glb') {
        const form = new FormData();
        form.append('file', fs.createReadStream(filePath));
        form.append('format', format);

        try {
            const response = await axios.post(`${this.baseURL}/api/task/create`, form, {
                headers: form.getHeaders()
            });

            return response.data;
        } catch (error) {
            throw new Error(`Upload failed: ${error.message}`);
        }
    }

    async getTaskStatus(taskId) {
        try {
            const response = await axios.get(`${this.baseURL}/api/task/${taskId}`);
            return response.data;
        } catch (error) {
            throw new Error(`Status check failed: ${error.message}`);
        }
    }

    async downloadResult(taskId, outputPath) {
        try {
            const response = await axios.get(`${this.baseURL}/api/task/${taskId}/download`, {
                responseType: 'stream'
            });

            return new Promise((resolve, reject) => {
                const writer = fs.createWriteStream(outputPath);
                response.data.pipe(writer);

                writer.on('finish', resolve);
                writer.on('error', reject);
            });
        } catch (error) {
            throw new Error(`Download failed: ${error.message}`);
        }
    }
}

// Usage
async function main() {
    const api = new AutoRigAPI();

    try {
        // Upload model
        console.log('Uploading model...');
        const uploadResult = await api.uploadModel('character.glb');
        const taskId = uploadResult.task_id;
        console.log(`Task started: ${taskId}`);

        // Wait and check status
        console.log('Waiting for completion...');
        let status;
        do {
            await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
            status = await api.getTaskStatus(taskId);
            console.log(`Status: ${status.status}`);
        } while (status.status === 'processing');

        // Download result
        if (status.status === 'completed') {
            console.log('Downloading result...');
            await api.downloadResult(taskId, 'rigged_character.glb');
            console.log('Success! Character rigged and downloaded.');
        } else {
            console.log('Rigging failed or is still processing');
        }

    } catch (error) {
        console.error('Error:', error.message);
    }
}

main();
```

## Supported Formats

- **GLB**: Web-optimized binary glTF format
- **FBX**: Autodesk FBX for Unity and Unreal Engine
- **OBJ**: Wavefront OBJ for static models

## Response Format

### Upload Response
```json
{
    "task_id": "abc123def456",
    "status": "processing",
    "estimated_time": 300,
    "message": "Model uploaded successfully, rigging in progress"
}
```

### Status Response
```json
{
    "task_id": "abc123def456",
    "status": "completed",
    "progress": 100,
    "result_url": "https://autorig.online/u/abc123/result.glb",
    "created_at": "2024-12-20T10:30:00Z",
    "completed_at": "2024-12-20T10:35:00Z"
}
```

## Error Handling

The API returns appropriate HTTP status codes:

- `200`: Success
- `400`: Bad request (invalid file, unsupported format)
- `404`: Task not found
- `429`: Rate limit exceeded
- `500`: Server error

## Rate Limits

- Free tier: 3 conversions per IP address
- Paid plans available for higher limits
- Contact support for enterprise solutions

## Support

For API questions, visit our [FAQ](https://autorig.online/faq) or contact support@autorig.online.

## License

This repository is for educational purposes. The AutoRig Online service has its own terms of service.
