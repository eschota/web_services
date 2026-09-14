namespace SandFlow.Server;

public static class SnapshotHttp
{
    private static readonly SemaphoreSlim Slots=new(2,2);
    public sealed class Body:IDisposable
    {
        internal Body(SnapshotUpload upload,byte[] payload){Upload=upload;Payload=payload;}
        public SnapshotUpload Upload {get;}
        public byte[] Payload {get;private set;}
        private int disposed;
        public void Dispose(){if(Interlocked.Exchange(ref disposed,1)==0){Payload=[];Slots.Release();}}
    }
    public static async Task<Body> Read(HttpContext context)
    {
        long Header(string key)=>long.TryParse(context.Request.Headers[key],out var n)&&n>=0?n:throw new ApiFailure(400,"snapshot_header");
        var upload=new SnapshotUpload(Header("X-SF-Epoch"),Header("X-SF-Tick"),Header("X-SF-Sequence"),
            Header("X-SF-Revision"),context.Request.Headers["X-SF-SHA256"].ToString());
        if(context.Request.ContentLength is null or >Protocol.MaxSnapshotBytes or <16)throw new ApiFailure(413,"snapshot_size");
        var expected=(int)context.Request.ContentLength.Value;
        if(!await Slots.WaitAsync(0,context.RequestAborted))throw new ApiFailure(429,"snapshot_capacity");
        try
        {
            using var timeout=CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);timeout.CancelAfter(TimeSpan.FromSeconds(30));
            var payload=new byte[expected];var offset=0;
            while(offset<expected)
            {
                var count=await context.Request.Body.ReadAsync(payload.AsMemory(offset),timeout.Token);
                if(count==0)throw new ApiFailure(400,"snapshot_truncated");offset+=count;
            }
            if(await context.Request.Body.ReadAsync(new byte[1],timeout.Token)!=0)throw new ApiFailure(413,"snapshot_size");
            return new Body(upload,payload);
        }
        catch(OperationCanceledException)when(!context.RequestAborted.IsCancellationRequested){Slots.Release();throw new ApiFailure(408,"snapshot_timeout");}
        catch{Slots.Release();throw;}
    }
}
