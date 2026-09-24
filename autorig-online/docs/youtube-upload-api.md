# YouTube uploads for `@unlim3d`

This is the operator guide for connecting the AutoRig backend and using the
upload API documented at `https://autorig.online/dev`.

## Fixed channel and account

- Channel: **U3d Indie Game Developer**, handle `@unlim3d`.
- Channel ID: `UCpCN8wm6UXr8Ke_m-zSaThQ` (verified from the channel's YouTube
  Studio link).
- Google account used for the Cloud project and channel OAuth: `cgteamorg@gmail.com`.
- The OAuth callback refuses to store a refresh token unless `channels.list(mine=true)`
  returns the fixed channel ID above. If OAuth opens the wrong channel, switch
  channel in the Google/YouTube account chooser and retry.

## One-time Google Cloud setup

1. In Cloud Console, select `cgteamorg@gmail.com` and create a project for
   AutoRig YouTube uploads.
2. Enable **YouTube Data API v3** for that project.
3. Configure the OAuth consent screen with the app name and owner contact, and
   add `cgteamorg@gmail.com` as a test user while the app is in testing mode.
4. Add the `https://www.googleapis.com/auth/youtube.upload` scope.
5. Create an OAuth **Web application** client with this exact authorized
   redirect URI:
   `https://autorig.online/api/oauth/youtube/callback`.
6. Store the OAuth client ID and client secret in the production-only backend
   environment (`YOUTUBE_GOOGLE_CLIENT_ID` and
   `YOUTUBE_GOOGLE_CLIENT_SECRET`). Keep AutoRig sign-in's `GOOGLE_CLIENT_ID`
   and `GOOGLE_CLIENT_SECRET` unchanged. Do not put either client secret in
   source code, the browser, or Git.
7. Sign in to AutoRig with an administrator account and open
   `https://autorig.online/api/admin/youtube/oauth/start`. In Google's consent
   flow, select the `@unlim3d` channel and grant the upload scope. The callback
   verifies the channel ID and saves the refresh token in the server-side
   `youtube_credentials` database row.
8. Confirm connection through `GET /api/admin/youtube/status` while signed in as
   an administrator. Never print or return the token.

OAuth app verification may be required before Google permits access beyond
test users. In addition, uploads from a new or unverified YouTube API project
may be restricted to private visibility until YouTube completes its API
compliance audit. These are separate Google review gates.

## Upload through `/dev`

Only AutoRig administrators can upload. Create an API key while signed in as an
administrator on `/dev`; an API key owned by an ordinary user is rejected.
Send a multipart request to `POST /dev/api/youtube/videos`:

```sh
BASE=https://autorig.online
curl -X POST "$BASE/dev/api/youtube/videos" \
  -H "Authorization: Bearer YOUR_ADMIN_API_KEY" \
  -F "file=@video.mp4;type=video/mp4" \
  -F "title=Video title" \
  -F "description=Video description" \
  -F "tags=3D,game development,AutoRig" \
  -F "privacy_status=public"
```

Fields:

- `file`: video file (`video/*` or `application/octet-stream`). The configured
  default maximum is 4096 MB (`YOUTUBE_API_UPLOAD_MAX_MB`).
- `title`: required, 1 to 100 characters.
- `description`: optional, up to 5000 characters.
- `tags`: optional, comma-separated, up to 500 characters total.
- `privacy_status`: `public` (default), `unlisted`, or `private`.

Successful response:

```json
{"ok":true,"video_id":"VIDEO_ID","url":"https://www.youtube.com/watch?v=VIDEO_ID","privacy_status":"public","requested_privacy_status":"public"}
```

Use the returned URL to review the uploaded video in YouTube Studio. The API
uses resumable `videos.insert` uploads; it streams the request-scoped temporary
file to YouTube and closes it after the attempt. The API does not change
visibility later. `privacy_status` in the response reflects YouTube's returned
state; `requested_privacy_status` is what the client asked for. Public is the
default requested visibility. YouTube can force
uploads from a new or unverified API project to remain private until its
compliance audit passes; the request field cannot bypass that restriction.

## Shorts and long-form videos

There is no `isShort` flag in the upload request. YouTube classifies the video
from the media itself. Under the current rules for standard channels, a square
or vertical video up to three minutes can be categorized as a Short; longer or
landscape uploads are long-form. A Short and a long-form video use the same API
endpoint and request fields. Check YouTube's current rules before changing
duration guidance:

- [Understand three-minute YouTube Shorts](https://support.google.com/youtube/answer/15424877?hl=en)
- [YouTube Data API `videos.insert`](https://developers.google.com/youtube/v3/docs/videos/insert)

## Operational safeguards

- Never upload test or user-supplied material to the channel without the
  owner's direct request for that video.
- Never accept the Google OAuth grant on the owner's behalf. Stop at the Google
  consent screen and let the owner approve `youtube.upload` access.
- If channel verification fails, do not save the token; retry OAuth after
  selecting `@unlim3d`.
- Keep Google client credentials and refresh tokens server-side. Do not log
  OAuth response bodies or token values.
- YouTube API uploads use quota. Watch the API project's quota and upload
  eligibility before sending large batches.
