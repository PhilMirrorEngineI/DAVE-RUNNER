"""Connect the existing chat bubble to the existing read-only job report."""

CHAT_POLLING_SCRIPT = r'''
let pmeiChatBusy=false;
const pmeiPause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function pmeiJobPath(receipt){
  const id=receipt && receipt.job_id;
  if(typeof id!=='string'||!/^web-[a-f0-9]{12}$/.test(id))throw Error('Invalid job receipt');
  const path='/orchestration/jobs/'+id;
  if(receipt.status_url!==path)throw Error('Invalid job status address');
  return path;
}
async function pmeiFetch(url,options={},milliseconds=20000){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),milliseconds);
  try{return await fetch(url,{...options,signal:controller.signal})}
  finally{clearTimeout(timer)}
}
async function watchPmeiJob(receipt,onUpdate,get=pmeiFetch,pause=pmeiPause,limit=900){
  const path=pmeiJobPath(receipt);
  for(let count=0;count<limit;count++){
    const response=await get(path,{method:'GET',cache:'no-store'});
    const report=await response.json();
    if(!response.ok)throw Error(report.error||'Job status is unavailable');
    if(report.job_id!==receipt.job_id)throw Error('Job identity changed');
    onUpdate(report);
    const phase=report.automatic_continuation?.phase;
    if(!['QUEUED','RUNNING'].includes(phase)||report.result_status==='CONTINUATION_UNCONFIRMED')return report;
    await pause(2000);
  }
  throw Error('Watching paused. Reload to read this job again; do not resend the task.');
}
function pmeiRememberJob(receipt){
  try{sessionStorage.setItem('pmeiPendingJob',JSON.stringify({job_id:receipt.job_id,status_url:receipt.status_url}))}catch(e){}
}
function pmeiForgetJob(){try{sessionStorage.removeItem('pmeiPendingJob')}catch(e){}}
function pmeiCandidateBubble(){
  const wrap=document.getElementById('chatMessages'),bubble=document.createElement('div');
  bubble.className='msg foh';
  bubble.innerHTML='<div class="who">FRONT-OF-HOUSE DAVE</div><div class="meta"></div><div class="pmei-answer" style="white-space:pre-wrap"></div>';
  wrap.appendChild(bubble);
  return {show:d=>{
    bubble.querySelector('.meta').textContent=d.job_id?d.job_id+' · '+(d.result_status||'QUEUED'):'Dave';
    bubble.querySelector('.pmei-answer').textContent=d.foh_presentation?.text||d.text||d.delivery?.text||d.error||'Dave is working on this request.';
    wrap.scrollTop=wrap.scrollHeight;
  },fail:message=>{bubble.querySelector('.meta').textContent=message;}};
}
function pmeiSetBusy(busy){
  pmeiChatBusy=busy;
  document.getElementById('sendChat').disabled=busy;
  document.getElementById('sendDeterministic').disabled=busy;
}
async function sendChat(){
  if(pmeiChatBusy)return;
  const box=document.getElementById('chatInput'),msg=box.value.trim();
  if(!msg)return;
  pmeiSetBusy(true);lastPhilMessage=msg;
  const wrap=document.getElementById('chatMessages');
  wrap.insertAdjacentHTML('beforeend','<div class="msg"><div class="who">PHIL</div>'+esc(msg)+'</div>');
  box.value='';const bubble=pmeiCandidateBubble();
  const fd=new FormData();fd.append('message',msg);fd.append('history',JSON.stringify(chatHistory));
  let receipt=null;
  try{
    const response=await pmeiFetch('/chat',{method:'POST',body:fd},300000);
    let data=await response.json();bubble.show(data);
    if(response.status===202&&data.job_id){
      pmeiJobPath(data);receipt=data;pmeiRememberJob(data);
      data=await watchPmeiJob(data,bubble.show);pmeiForgetJob();
    }else if(!response.ok){return}
    const answer=data.foh_presentation?.text||data.text||data.delivery?.text;
    if(answer){chatHistory.push({role:'user',content:msg},{role:'assistant',content:answer});chatHistory=chatHistory.slice(-20)}
  }catch(e){
    bubble.fail(receipt?'Status reading paused for '+receipt.job_id+'. Reload to keep watching; no task was resubmitted.':'Request status uncertain. Check the server before submitting again.');
  }finally{pmeiSetBusy(false)}
}
async function resumePmeiWatching(){
  let receipt;
  try{receipt=JSON.parse(sessionStorage.getItem('pmeiPendingJob')||'null')}catch(e){pmeiForgetJob();return}
  if(!receipt||pmeiChatBusy)return;
  try{pmeiJobPath(receipt)}catch(e){pmeiForgetJob();return}
  pmeiSetBusy(true);const bubble=pmeiCandidateBubble();bubble.show(receipt);
  try{await watchPmeiJob(receipt,bubble.show);pmeiForgetJob()}
  catch(e){bubble.fail('Status reading paused for '+receipt.job_id+'. Reload to keep watching; no task was resubmitted.')}
  finally{pmeiSetBusy(false)}
}
setTimeout(resumePmeiWatching,0);
'''


def connect_chat_polling(page):
    lines = page.splitlines(keepends=True)
    matches = [i for i, line in enumerate(lines) if line.startswith("async function sendChat(){")]
    if len(matches) != 1 or not lines[matches[0]].rstrip().endswith("}"):
        raise ValueError("Unrecognised cockpit chat function; preserve the page for review.")
    lines[matches[0]] = CHAT_POLLING_SCRIPT + "\n"
    return "".join(lines)
