const puppeteer = require('puppeteer');

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  const page = await browser.newPage();
  
  const failed404s = [];
  const allRequests = [];
  
  page.on('response', async (response) => {
    const url = response.url();
    const status = response.status();
    allRequests.push({ url, status });
    
    if (status === 404 && url.includes('b3a13833-ad9b-48a6-9104-2041ce7eaee3')) {
      failed404s.push({ url, status });
      console.log(`❌ 404: ${url}`);
    }
  });
  
  page.on('console', msg => {
    const text = msg.text();
    if (text.includes('404') || text.includes('error') || text.includes('Error')) {
      console.log('Console:', text);
    }
  });
  
  console.log('Navigating to page...');
  await page.goto('https://autorig.online/task?id=b3a13833-ad9b-48a6-9104-2041ce7eaee3', {
    waitUntil: 'networkidle2',
    timeout: 30000
  });
  
  await page.waitForTimeout(2000);
  
  console.log('\n1. Switching to Anim mode...');
  const animButton = await page.$('button:has-text("Anim")');
  if (animButton) {
    await animButton.click();
  } else {
    const buttons = await page.$$('button');
    for (const btn of buttons) {
      const text = await page.evaluate(el => el.textContent, btn);
      if (text.includes('Anim')) {
        await btn.click();
        break;
      }
    }
  }
  
  await page.waitForTimeout(3000);
  
  console.log('\n2. Clicking first custom animation...');
  const customAnims = await page.$$('[data-animation-name], .animation-item');
  if (customAnims.length > 0) {
    await customAnims[0].click();
    await page.waitForTimeout(2000);
    
    console.log('Clicking second custom animation...');
    if (customAnims.length > 1) {
      await customAnims[1].click();
      await page.waitForTimeout(2000);
    }
  } else {
    console.log('Trying to find animation buttons another way...');
    const allButtons = await page.$$('button');
    let clickCount = 0;
    for (const btn of allButtons) {
      const text = await page.evaluate(el => el.textContent, btn);
      if (text && !text.includes('Rig') && !text.includes('Anim') && text.trim().length > 0) {
        await btn.click();
        await page.waitForTimeout(1500);
        clickCount++;
        if (clickCount >= 2) break;
      }
    }
  }
  
  await page.waitForTimeout(2000);
  
  console.log('\n=== SUMMARY ===');
  console.log(`Total requests monitored: ${allRequests.length}`);
  console.log(`404 errors found: ${failed404s.length}`);
  
  if (failed404s.length > 0) {
    console.log('\n❌ 404 Errors:');
    failed404s.forEach(req => {
      console.log(`  - ${req.url}`);
    });
  } else {
    console.log('✅ No 404 errors detected');
  }
  
  const fbmRequests = allRequests.filter(r => r.url.includes('.fbm'));
  if (fbmRequests.length > 0) {
    console.log(`\n.fbm requests: ${fbmRequests.length}`);
    fbmRequests.forEach(r => console.log(`  ${r.status}: ${r.url}`));
  }
  
  await browser.close();
})();
