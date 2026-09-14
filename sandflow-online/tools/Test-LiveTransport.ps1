$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$secret=(Get-Content -LiteralPath (Join-Path $root '.work\qa-session\server.env') -Raw).Trim().Split('=',2)[1]
$base='https://autorig.online/sandflow/api/v1'
$first=Invoke-RestMethod -Method Post -Uri "$base/sessions/guest" -Headers @{'X-SF-QA-Token'=$secret} -ContentType 'application/json' -Body '{}'
$second=Invoke-RestMethod -Method Post -Uri "$base/sessions/guest" -Headers @{'X-SF-QA-Token'=$secret} -ContentType 'application/json' -Body '{}'
$password=[Guid]::NewGuid().ToString('N')
$created=Invoke-RestMethod -Method Post -Uri "$base/worlds" -Headers @{Authorization="Bearer $($first.token)"} -ContentType 'application/json' -Body (@{isPrivate=$true;password=$password;demo=$true;mode='coop';map='twin-shore-river';teamSize=1}|ConvertTo-Json)
$world=$created.world.id
$null=Invoke-RestMethod -Method Post -Uri "$base/worlds/$world/join" -Headers @{Authorization="Bearer $($second.token)"} -ContentType 'application/json' -Body (@{password=$password}|ConvertTo-Json)
$sockets=[Collections.Generic.List[Net.WebSockets.ClientWebSocket]]::new()
$deadline=[Threading.CancellationTokenSource]::new(60000)
function Connect-Socket($token,$channel){
    $socket=[Net.WebSockets.ClientWebSocket]::new();$socket.Options.SetRequestHeader('Authorization',"Bearer $token")
    $null=$socket.ConnectAsync([Uri]"wss://autorig.online/sandflow/ws/$world/$channel",$deadline.Token).GetAwaiter().GetResult()
    Write-Host "Connected $channel WebSocket: $($socket.State)"
    $sockets.Add($socket);return $socket
}
function Receive-Message($socket){
    $buffer=[byte[]]::new(1048576);$offset=0
    do{$part=$socket.ReceiveAsync([ArraySegment[byte]]::new($buffer,$offset,$buffer.Length-$offset),$deadline.Token).GetAwaiter().GetResult();$offset+=$part.Count}while(-not $part.EndOfMessage)
    if($part.MessageType -eq [Net.WebSockets.WebSocketMessageType]::Close){throw 'Unexpected socket close'}
    if($part.MessageType -eq [Net.WebSockets.WebSocketMessageType]::Binary){return ,$buffer[0..($offset-1)]}
    $message=[Text.Encoding]::UTF8.GetString($buffer,0,$offset)|ConvertFrom-Json
    Write-Host "Received $($message.type), epoch=$($message.epoch), tick=$($message.tick)"
    return $message
}
function Send-Message($socket,$value){
    $bytes=[Text.Encoding]::UTF8.GetBytes(($value|ConvertTo-Json -Depth 8 -Compress))
    Write-Host "Sending $($value.type)"
    $null=$socket.SendAsync([ArraySegment[byte]]::new($bytes),[Net.WebSockets.WebSocketMessageType]::Text,$true,$deadline.Token).GetAwaiter().GetResult()
}
try{
    $a=Connect-Socket $first.token 'control';$welcome=Receive-Message $a
    if($welcome.type -ne 'welcome'){throw 'No first welcome'}
    $b=Connect-Socket $second.token 'control';$other=Receive-Message $b
    if($other.type -ne 'welcome'){throw 'No second welcome'}
    $aState=Connect-Socket $first.token 'state';$bState=Connect-Socket $second.token 'state'
    Send-Message $a @{type='ready';tuningDefaultsHash=$welcome.data.tuningDefaultsHash;version=2;epoch=$welcome.epoch;tick=0;sequence=0}
    do{$hostEvent=Receive-Message $a}while($hostEvent.type -ne 'host')
    Send-Message $b @{type='ready';tuningDefaultsHash=$welcome.data.tuningDefaultsHash;version=2;epoch=$welcome.epoch;tick=0;sequence=0}
    Send-Message $a @{type='heartbeat';version=2;epoch=$welcome.epoch;tick=0;sequence=0}
    Send-Message $b @{type='input';version=2;epoch=$welcome.epoch;sequence=1;command=@{kind='simulation';action='enqueue';commandType=3;amount=.01;x=2;z=2;radius=.25}}
    do{$order=Receive-Message $a}while($order.type -ne 'ordered')
    if($order.data.participantId -ne $created.participantId -and $order.data.participantId -ne $second.identity.id){throw 'Unknown sender'}
    Send-Message $a @{type='heartbeat';version=2;epoch=$welcome.epoch;tick=0;sequence=0}
    Send-Message $a @{type='commitBatch';version=2;epoch=$welcome.epoch;commits=@(@{tick=1;sequence=$order.sequence;substeps=1},@{tick=2;sequence=$order.sequence;substeps=2})}
    do{$commit=Receive-Message $b}while($commit.type -ne 'committedBatch')
    if($commit.tick -ne 2 -or $commit.sequence -ne $order.sequence){throw 'Commit cursor mismatch'}
    $frame=[byte[]]::new(48);$frame[0]=83;$frame[1]=70;$frame[2]=79;$frame[3]=2;$frame[4]=1
    [Buffer]::BlockCopy([BitConverter]::GetBytes([long]$welcome.epoch),0,$frame,8,8)
    [Buffer]::BlockCopy([BitConverter]::GetBytes([long]2),0,$frame,16,8);$frame[24]=1;$frame[30]=1
    $null=$aState.SendAsync([ArraySegment[byte]]::new($frame),[Net.WebSockets.WebSocketMessageType]::Binary,$true,$deadline.Token).GetAwaiter().GetResult()
    $echo=Receive-Message $bState
    if([Convert]::ToBase64String($echo) -ne [Convert]::ToBase64String($frame)){throw 'State channel changed bytes'}
    $report=[ordered]@{schemaVersion=1;checkedUtc=[DateTime]::UtcNow.ToString('o');status='PASS';scope='Actual HTTPS admission and dual WSS relay, not GPU simulation';privateWorld=$world;participants=2;connections=4;committedTick=2;stateBytes=48}
    $report|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $root '.work\qa-session\transport-report.json') -Encoding utf8
    Write-Output 'PASS live private admission, two control sockets, ordered input, batched commits and two state sockets.'
}
finally{foreach($socket in $sockets){$socket.Abort();$socket.Dispose()};$deadline.Dispose()}
