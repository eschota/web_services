using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using SandFlow.Protocol;

if(args.Length!=2)throw new ArgumentException("Usage: DepartureLiveProbe project-local-qa-env project-local-output");
var project=FindProject();
string Local(string path){var value=Path.GetFullPath(path);if(!value.StartsWith(project+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new ArgumentException("Project-local paths required");return value;}
var secret=File.ReadAllText(Local(args[0])).Trim().Split('=',2)[1];
var output=Local(args[1]);Directory.CreateDirectory(Path.GetDirectoryName(output)!);
using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=timeout.Token;
using var http=new HttpClient(new HttpClientHandler{AllowAutoRedirect=false,UseCookies=false}){BaseAddress=new Uri("https://autorig.online/sandflow/api/v1/"),Timeout=TimeSpan.FromSeconds(15)};
var checks=new List<string>();
async Task<JsonElement> Request(string path,object? body,string? bearer=null,bool qa=false,byte[]? bytes=null,Dictionary<string,string>? headers=null)
{
    using var request=new HttpRequestMessage(body==null&&bytes==null?HttpMethod.Get:HttpMethod.Post,path);
    if(qa)request.Headers.Add("X-SF-QA-Token",secret);
    if(bearer!=null)request.Headers.Authorization=new AuthenticationHeaderValue("Bearer",bearer);
    if(bytes!=null){request.Content=new ByteArrayContent(bytes);request.Content.Headers.ContentType=new("application/octet-stream");}
    else if(body!=null)request.Content=new StringContent(JsonSerializer.Serialize(body),Encoding.UTF8,"application/json");
    if(headers!=null)foreach(var pair in headers)request.Headers.Add(pair.Key,pair.Value);
    using var response=await http.SendAsync(request,token);var text=await response.Content.ReadAsStringAsync(token);
    if(!response.IsSuccessStatusCode)
    {
        var code="http_"+(int)response.StatusCode;
        try{code=JsonDocument.Parse(text).RootElement.GetProperty("error").GetString()??code;}catch{}
        throw new InvalidOperationException(code);
    }
    return JsonDocument.Parse(text).RootElement.Clone();
}
async Task<ClientWebSocket> Socket(string world,string channel,string bearer)
{
    var socket=new ClientWebSocket();socket.Options.SetRequestHeader("Authorization","Bearer "+bearer);
    await socket.ConnectAsync(new Uri("wss://autorig.online/sandflow/ws/"+world+"/"+channel),token);return socket;
}
async Task Send(ClientWebSocket socket,object value)=>await socket.SendAsync(JsonSerializer.SerializeToUtf8Bytes(value),WebSocketMessageType.Text,true,token);
async Task<byte[]> Read(ClientWebSocket socket)
{
    using var data=new MemoryStream();var buffer=new byte[8192];WebSocketReceiveResult read;
    do{read=await socket.ReceiveAsync(new ArraySegment<byte>(buffer),token);if(read.MessageType==WebSocketMessageType.Close)throw new InvalidOperationException("socket_closed");data.Write(buffer,0,read.Count);if(data.Length>65536)throw new InvalidOperationException("probe_message_limit");}while(!read.EndOfMessage);
    return data.ToArray();
}
async Task<JsonElement> Event(ClientWebSocket socket,string type)
{
    for(var i=0;i<100;i++){var value=JsonDocument.Parse(await Read(socket)).RootElement.Clone();if(value.GetProperty("type").GetString()==type)return value;}
    throw new InvalidOperationException("event_window_exhausted");
}
void Check(bool condition,string label){if(!condition)throw new InvalidOperationException(label);checks.Add(label);}
byte[] Snapshot(string world,long epoch)
{
    var state=new WorldSnapshot{WorldId=world,Epoch=epoch,Tick=1,Sequence=1,FixedDt=1f/240f,SimTime=1d/240,ResX=16,ResZ=16,CellSize=.125f,MetadataJson="{\"scope\":\"protocol-only-private-departure-canary\"}"};
    state.Fields.Add(new SnapshotField{Name="WaterDepth",Stride=4,Data=new byte[1024]});return SnapshotCodec.Encode(state);
}
Dictionary<string,string> UploadHeaders(byte[] data,long epoch,long revision)=>new(){["X-SF-Epoch"]=epoch.ToString(),["X-SF-Tick"]="1",["X-SF-Sequence"]="1",["X-SF-Revision"]=revision.ToString(),["X-SF-SHA256"]=Convert.ToHexString(SHA256.HashData(data))};
var owner=await Request("sessions/guest",new{},qa:true);var ownerToken=owner.GetProperty("token").GetString()!;
var password=Guid.NewGuid().ToString("N");
var admission=await Request("worlds",new{isPrivate=true,password,mode="coop",map="twin-shore-river",demo=true,teamSize=1},ownerToken);
var world=admission.GetProperty("world").GetProperty("id").GetString()!;
using var host=await Socket(world,"control",ownerToken);var welcome=await Event(host,"welcome");
Check(welcome.GetProperty("data").GetProperty("departureProtocol").GetInt32()==1,"departure capability advertised");
await Send(host,new{type="ready",version=1,epoch=1,tick=0,sequence=0});await Event(host,"host");
var guest=await Request("sessions/guest",new{},qa:true);var guestToken=guest.GetProperty("token").GetString()!;
await Request("worlds/"+world+"/join",new{password},guestToken);
using var peer=await Socket(world,"control",guestToken);await Event(peer,"welcome");
await Send(peer,new{type="ready",version=1,epoch=1,tick=0,sequence=0});await Event(peer,"presence");
using var hostState=await Socket(world,"state",ownerToken);using var peerState=await Socket(world,"state",guestToken);
await Send(host,new{type="input",version=1,epoch=1,sequence=1,command=new{kind="simulation",action="enqueue",commandType=1}});await Event(host,"ordered");
var operation=Guid.NewGuid().ToString("N");
await Send(host,new{type="depart_begin",version=1,epoch=1,tick=0,sequence=0,operationId=operation});
var prepared=await Event(host,"depart_prepared");
Check(prepared.GetProperty("data").GetProperty("tick").GetInt64()==1&&prepared.GetProperty("data").GetProperty("sequence").GetInt64()==1,"live fence includes pending ordered input");
await Send(host,new{type="commit",version=1,epoch=1,tick=1,sequence=1,substeps=1});await Event(host,"committed");
var payload=Snapshot(world,1);var frame=new byte[32+payload.Length];
frame[0]=(byte)'S';frame[1]=(byte)'F';frame[2]=(byte)'O';frame[3]=1;frame[4]=1;
BitConverter.GetBytes(1L).CopyTo(frame,8);BitConverter.GetBytes(1L).CopyTo(frame,16);BitConverter.GetBytes(1u).CopyTo(frame,24);BitConverter.GetBytes((ushort)1).CopyTo(frame,30);payload.CopyTo(frame,32);
await hostState.SendAsync(frame,WebSocketMessageType.Binary,true,token);
Check((await Read(peerState)).AsSpan().SequenceEqual(frame),"fresh snapshot crosses the separate live state channel");
var receipt=await Request("worlds/"+world+"/departures/"+operation,null,ownerToken,bytes:payload,headers:UploadHeaders(payload,1,0));
var retry=await Request("worlds/"+world+"/departures/"+operation,null,ownerToken,bytes:payload,headers:UploadHeaders(payload,1,0));
Check(receipt.GetProperty("snapshot").GetProperty("revision").GetInt64()==1&&retry.GetProperty("snapshot").GetProperty("revision").GetInt64()==1,"HTTP completion retry keeps one durable version");
var restored=await Event(peer,"restore_required");
Check(restored.GetProperty("epoch").GetInt64()==2&&restored.GetProperty("tick").GetInt64()==1&&restored.GetProperty("data").GetProperty("lostThroughTick").GetInt64()==1,"remaining socket receives zero-loss cloud recovery");
await Send(peer,new{type="ready",version=1,epoch=2,tick=1,sequence=1});var elected=await Event(peer,"host");
Check(elected.GetProperty("data").GetProperty("participantId").GetString()==guest.GetProperty("identity").GetProperty("id").GetString(),"remaining participant becomes authority on same control socket");
// Last participant also uses the transactional path, leaving no running canary.
for(var second=0;second<10;second++)
{
    await Send(peer,new{type="heartbeat",version=1,epoch=2,tick=1,sequence=1});
    await Task.Delay(TimeSpan.FromSeconds(1),token);
}
var lastOperation=Guid.NewGuid().ToString("N");
await Send(peer,new{type="depart_begin",version=1,epoch=2,tick=1,sequence=1,operationId=lastOperation});await Event(peer,"depart_prepared");
var finalPayload=Snapshot(world,2);var final=await Request("worlds/"+world+"/departures/"+lastOperation,null,guestToken,bytes:finalPayload,headers:UploadHeaders(finalPayload,2,1));
Check(final.GetProperty("snapshot").GetProperty("revision").GetInt64()==2,"last participant saves and leaves");
var status=await Request("worlds/"+world+"/departures/"+lastOperation,null,guestToken);
Check(status.GetProperty("snapshot").GetProperty("sha256").GetString()==Convert.ToHexString(SHA256.HashData(finalPayload)),"durable receipt remains readable after departure");
var result=new{success=true,worldId=world,isPrivate=true,checks,scope="Production HTTP and dual-WSS protocol canary with CPU fixture; no Unity gameplay or voice proof",completedUtc=DateTimeOffset.UtcNow};
File.WriteAllText(output,JsonSerializer.Serialize(result,new JsonSerializerOptions{WriteIndented=true}));Console.WriteLine(JsonSerializer.Serialize(result));

static string FindProject(){var dir=new DirectoryInfo(AppContext.BaseDirectory);while(dir!=null){if(File.Exists(Path.Combine(dir.FullName,"server","SandFlow.Server.csproj")))return dir.FullName;dir=dir.Parent;}throw new InvalidOperationException("Project root missing");}
