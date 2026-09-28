# Separate YouTube upload destinations

AutoRig is an independent service with its own YouTube channel and automatic
task uploader. Its `youtube_credentials` database row and original Google OAuth
client remain dedicated to AutoRig. The U3D provider API never reads or writes
that row and never receives AutoRig task uploads.

## U3D provider channel

The optional provider destination is **U3d Indie Game Developer** (`@unlim3d`,
channel ID `UCpCN8wm6UXr8Ke_m-zSaThQ`). Its token is stored separately in
`u3d_youtube_credentials`, using OAuth client variables
`U3D_YOUTUBE_CLIENT_ID` and `U3D_YOUTUBE_CLIENT_SECRET` and callback
`https://autorig.online/api/oauth/u3d-youtube/callback`. OAuth uses only the
`youtube.upload` scope. The Google Cloud redirect URI must match exactly.

The route is `POST https://autorig.online/dev/api/youtube/videos`. It uses a
dedicated provider credential from the server-only `U3D_YOUTUBE_AGENT_KEYS`
allowlist. This is not an AutoRig admin API key and has no access to AutoRig
tasks or administration. Requests use `Authorization: Bearer <U3D_AGENT_KEY>`
(the legacy `X-U3D-Agent-Key` header remains accepted) plus multipart fields
`file`, `title`, `description`, `tags` (comma-separated), and
`privacy_status` (`public`, `unlisted`, or `private`; default `public`). The
handler streams its request-scoped upload spool to YouTube's resumable upload
and closes it.

The owner can copy the dedicated key from `https://autorig.online/dev/youtube`
while signed in as an AutoRig administrator, then provide it to any explicitly
authorized agent on any computer. Agents should read the stable machine-readable
contract at `https://autorig.online/dev/youtube/skill.md`. Public readiness is
available from `GET https://autorig.online/dev/api/youtube/status`; neither
endpoint exposes credentials.

The initial OAuth connection may be completed by an administrator at
`/api/admin/u3d-youtube/oauth/start`. Select the `@unlim3d` Brand Account in
Google's chooser. Validate the returned `channel_id` on an upload before
enabling an agent key. Shorts are classified by YouTube from the video format,
aspect ratio, and duration; there is no Shorts API flag. Long videos use the
same resumable API.

Never store client secrets or refresh tokens in Git, browser storage, task
output, or logs. Report the actual privacy status returned by YouTube.
