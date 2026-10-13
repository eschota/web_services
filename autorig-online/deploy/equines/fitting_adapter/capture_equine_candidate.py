"""Farm Unity evidence for one hash-pinned engineering clip, never promotion."""
import argparse,hashlib,json,os,pathlib,subprocess,sys,time

def sha(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--bundle',required=True);parser.add_argument('--inputs',required=True);args=parser.parse_args()
    inp=pathlib.Path(args.inputs); job=json.loads((inp/'capture.json').read_text()); glb=inp/'candidate.glb'
    if job.get('scope')!='engineering_candidate_not_accepted' or sha(glb)!=job['candidate_sha256']:raise ValueError('candidate identity mismatch')
    sys.path.insert(0,args.bundle);import viewer_saver_box as saver
    from viewer_capture import ffmpeg_bin
    manifest=saver.live_manifest('test')
    if not manifest or manifest.get('tag')!=job['saver_tag']:raise ValueError('live Unity build changed; rebind before capture')
    exe,install=saver.ensure(manifest); out=pathlib.Path.cwd(); status=out/'saver_status.json'; log=out/'saver_player.log'
    command=[str(exe),'-batchmode','-logFile',str(log),'--run',job['run_id'],'--glb',str(glb.resolve()),
             '--clip',job['clip'],'--out',str(out/'preview.mp4'),'--poster',str(out/'poster.jpg'),'--status',str(status),
             '--secs','9','--fps','60','--size','1080x1920','--maxmb','8','--timeout','240','--ffmpeg',ffmpeg_bin(),
             '--scene','none','--yaw','65','--pitch','4','--turns','0.05','--quality','ultra','--dof','0']
    begin=time.time(); proc=subprocess.run(command,cwd=exe.parent,timeout=440,capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    value=json.loads(status.read_text()) if status.exists() else {'ok':False,'error':'missing status'}
    if sha(glb)!=job['candidate_sha256']:raise ValueError('source input changed during Unity capture')
    record={'schema':'autorig.equine-candidate-unity-capture/1','scope':job['scope'],'task_id':job['task_id'],'run_id':job['run_id'],
            'candidate_glb_sha256':sha(glb),'source_reference_video_sha256':job['source_reference_video_sha256'],
            'clip_requested':job['clip'],'source_override':str(glb.resolve()),'command':command,'saver':value,'saver_tag':manifest['tag'],
            'saver_manifest_sha256':manifest.get('sha256'),'saver_install':install,'exit_code':proc.returncode,
            'wall_seconds':time.time()-begin,'qa_decision':job['qa_decision'],'owner_acceptance':False,'activation_allowed':False}
    (out/'capture-receipt.json').write_text(json.dumps(record,indent=2)+'\n')
    ok=proc.returncode==0 and value.get('ok') and value.get('clip')==job['clip'] and value.get('frames')==540 and (out/'preview.mp4').is_file()
    print(json.dumps({'ok':ok,'clip':value.get('clip'),'frames':value.get('frames'),'source_sha256':sha(glb)}))
    return 0 if ok else 2

if __name__=='__main__':raise SystemExit(main())
