const { chromium } = require('playwright');

async function verifyAnimations() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Track network requests
  const requests = [];
  const errors = [];
  
  page.on('request', request => {
    const url = request.url();
    if (url.includes('/api/task/') || url.includes('.glb') || url.includes('.fbx') || 
        url.includes('texture') || url.includes('animation') || url.includes('.fbm')) {
      requests.push({ url, method: request.method(), timestamp: Date.now() });
    }
  });
  
  page.on('response', response => {
    const url = response.url();
    if (url.includes('/api/task/') && response.status() === 404) {
      errors.push({ url, status: 404, type: 'API 404 Error' });
    }
  });
  
  page.on('console', msg => {
    if (msg.type() === 'error') {
      errors.push({ type: 'Console Error', message: msg.text() });
    }
  });

  console.log('=== Starting Verification ===\n');

  const results = {
    pass: true,
    checks: {
      modeSwitchToAnim: { status: 'PENDING', details: '' },
      animationListVisible: { status: 'PENDING', details: '' },
      sizeConsistency: { status: 'PENDING', details: '' },
      texturesPresent: { status: 'PENDING', details: '' },
      noApiErrors: { status: 'PENDING', details: '' }
    }
  };

  try {
    // Navigate to the task page
    console.log('Step 1: Navigating to task page...');
    await page.goto('https://autorig.online/task?id=b3a13833-ad9b-48a6-9104-2041ce7eaee3', {
      waitUntil: 'networkidle',
      timeout: 30000
    });
    console.log('✓ Page loaded\n');

    // Wait for initial load
    await page.waitForTimeout(3000);

    // Step 1: Switch to Anim mode
    console.log('Step 2: Switching to Anim mode...');
    
    // Try multiple selectors for the Anim button
    const possibleSelectors = [
      'button:text("Anim")',
      '.mode-btn:has-text("Anim")',
      '[data-mode="anim"]',
      'button:has-text("Anim")',
      '.viewer-mode-btn:has-text("Anim")'
    ];
    
    let animButtonFound = false;
    for (const selector of possibleSelectors) {
      try {
        const btn = page.locator(selector).first();
        if (await btn.count() > 0) {
          await btn.click();
          console.log(`✓ Clicked Anim button using selector: ${selector}`);
          results.checks.modeSwitchToAnim.status = 'PASS';
          results.checks.modeSwitchToAnim.details = 'Successfully switched to Anim mode';
          animButtonFound = true;
          await page.waitForTimeout(2000);
          break;
        }
      } catch (e) {
        // Try next selector
      }
    }
    
    if (!animButtonFound) {
      console.log('⚠ Anim button not found with standard selectors');
      console.log('Attempting to get page structure...');
      
      // Get all buttons on the page
      const buttons = await page.locator('button').all();
      console.log(`Found ${buttons.length} buttons on page`);
      
      for (let i = 0; i < Math.min(buttons.length, 20); i++) {
        const text = await buttons[i].textContent();
        console.log(`  Button ${i}: "${text}"`);
        if (text && (text.includes('Anim') || text.includes('anim'))) {
          await buttons[i].click();
          console.log(`✓ Clicked button with text: "${text}"`);
          results.checks.modeSwitchToAnim.status = 'PASS';
          animButtonFound = true;
          await page.waitForTimeout(2000);
          break;
        }
      }
    }

    if (!animButtonFound) {
      results.checks.modeSwitchToAnim.status = 'FAIL';
      results.checks.modeSwitchToAnim.details = 'Could not find Anim mode button';
      results.pass = false;
    }

    await page.screenshot({ path: '/root/screenshot_after_anim_click.png' });

    // Step 2: Get list of custom animations
    console.log('\nStep 3: Looking for custom animations list...');
    await page.waitForTimeout(1500);
    
    // Save page HTML for analysis
    const html = await page.content();
    require('fs').writeFileSync('/root/page_content.html', html);
    console.log('Saved page HTML to /root/page_content.html');
    
    // Try multiple selectors for animation items
    const animSelectors = [
      '.custom-anim-card',
      '[data-animation-id]',
      '.animation-item',
      '.anim-item',
      '[class*="animation"][class*="item"]',
      '#animations-list li',
      '.animations-panel li',
      '[data-anim]',
      'li[onclick*="anim"]',
      'div[class*="anim"][onclick]'
    ];
    
    let animationItems = [];
    for (const selector of animSelectors) {
      const items = await page.locator(selector).all();
      if (items.length > 0) {
        console.log(`Found ${items.length} items with selector: ${selector}`);
        animationItems = items;
        break;
      }
    }

    if (animationItems.length === 0) {
      console.log('⚠ No animation items found with standard selectors');
      console.log('Searching for any clickable animation elements...');
      
      // Look for elements containing "custom" or animation names
      const customElements = await page.locator('*:has-text("custom_")').all();
      console.log(`Found ${customElements.length} elements containing "custom_"`);
      
      if (customElements.length > 0) {
        animationItems = customElements.slice(0, 5); // Take first 5
      }
    }

    console.log(`Total animation items found: ${animationItems.length}`);
    
    if (animationItems.length < 2) {
      results.checks.animationListVisible.status = 'FAIL';
      results.checks.animationListVisible.details = `Only found ${animationItems.length} animation items`;
      results.pass = false;
    } else {
      results.checks.animationListVisible.status = 'PASS';
      results.checks.animationListVisible.details = `Found ${animationItems.length} animation items`;
    }

    // Step 3: Test 2-3 custom animations
    if (animationItems.length >= 2) {
      console.log('\nStep 4: Testing animation switches...');
      
      // Get canvas element
      const canvas = page.locator('canvas').first();
      const canvasExists = await canvas.count() > 0;
      
      if (!canvasExists) {
        console.log('⚠ No canvas element found');
        results.checks.sizeConsistency.status = 'FAIL';
        results.checks.sizeConsistency.details = 'No canvas element found';
        results.pass = false;
      } else {
        const initialBounds = await canvas.boundingBox();
        console.log(`Initial canvas size: ${initialBounds?.width}x${initialBounds?.height}`);
        
        const animsToTest = Math.min(3, animationItems.length);
        const sizeMeasurements = [];
        const testedAnimations = [];
        
        for (let i = 0; i < animsToTest; i++) {
          console.log(`\n   Testing animation ${i + 1}/${animsToTest}...`);
          
          // Get animation name/id if possible
          const animText = await animationItems[i].textContent();
          const animId = await animationItems[i].getAttribute('data-animation-id').catch(() => null);
          const animName = animId || animText?.trim().substring(0, 30) || `Animation ${i + 1}`;
          console.log(`   Animation: ${animName}`);
          testedAnimations.push(animName);
          
          // Clear previous request tracking
          const requestsBefore = requests.length;
          const errorsBefore = errors.length;
          
          // Click animation
          try {
            await animationItems[i].click();
            console.log(`   ✓ Clicked animation`);
          } catch (e) {
            console.log(`   ⚠ Failed to click: ${e.message}`);
            continue;
          }
          
          // Wait for animation to load
          await page.waitForTimeout(2000);
          
          // Check canvas size
          const currentBounds = await canvas.boundingBox();
          if (currentBounds && initialBounds) {
            const widthRatio = currentBounds.width / initialBounds.width;
            const heightRatio = currentBounds.height / initialBounds.height;
            const sizeRatio = (currentBounds.width * currentBounds.height) / (initialBounds.width * initialBounds.height);
            sizeMeasurements.push(sizeRatio);
            console.log(`   Canvas size: ${currentBounds.width}x${currentBounds.height}`);
            console.log(`   Size ratio vs initial: ${sizeRatio.toFixed(3)} (width: ${widthRatio.toFixed(3)}, height: ${heightRatio.toFixed(3)})`);
            
            // Check if size changed dramatically (more than 30% smaller or 70% larger)
            if (sizeRatio < 0.7 || sizeRatio > 1.7) {
              console.log(`   ⚠ WARNING: Significant size change detected!`);
              if (results.checks.sizeConsistency.status !== 'FAIL') {
                results.checks.sizeConsistency.status = 'FAIL';
                results.checks.sizeConsistency.details = `${animName} changed size by ${((sizeRatio - 1) * 100).toFixed(0)}%`;
                results.pass = false;
              }
            }
          }
          
          // Check for new requests
          const newRequests = requests.slice(requestsBefore);
          console.log(`   New requests made: ${newRequests.length}`);
          newRequests.forEach(req => {
            const shortUrl = req.url.replace('https://autorig.online', '');
            console.log(`     - ${req.method} ${shortUrl}`);
          });
          
          // Check for new errors
          const newErrors = errors.slice(errorsBefore);
          if (newErrors.length > 0) {
            console.log(`   ⚠ Errors detected: ${newErrors.length}`);
            newErrors.forEach(err => {
              const shortUrl = err.url ? err.url.replace('https://autorig.online', '') : '';
              console.log(`     - ${err.type}: ${shortUrl || err.message}`);
            });
          }
          
          // Take screenshot after each animation
          await page.screenshot({ path: `/root/screenshot_anim_${i + 1}.png` });
        }
        
        // Analyze size consistency results
        if (sizeMeasurements.length > 0) {
          const avgSize = sizeMeasurements.reduce((a, b) => a + b, 0) / sizeMeasurements.length;
          const maxDeviation = Math.max(...sizeMeasurements.map(s => Math.abs(s - 1)));
          
          if (results.checks.sizeConsistency.status === 'PENDING') {
            if (maxDeviation < 0.3) {
              results.checks.sizeConsistency.status = 'PASS';
              results.checks.sizeConsistency.details = `Model size consistent (max deviation: ${(maxDeviation * 100).toFixed(1)}%)`;
            } else {
              results.checks.sizeConsistency.status = 'WARN';
              results.checks.sizeConsistency.details = `Some size variation detected (max deviation: ${(maxDeviation * 100).toFixed(1)}%)`;
            }
          }
        } else if (results.checks.sizeConsistency.status === 'PENDING') {
          results.checks.sizeConsistency.status = 'UNKNOWN';
          results.checks.sizeConsistency.details = 'Could not measure canvas size';
        }
        
        // Check textures (basic check - look for texture-related requests)
        const textureRequests = requests.filter(r => 
          r.url.includes('.jpg') || r.url.includes('.png') || 
          r.url.includes('texture') || r.url.includes('.fbm')
        );
        
        if (textureRequests.length > 0) {
          results.checks.texturesPresent.status = 'PASS';
          results.checks.texturesPresent.details = `Loaded ${textureRequests.length} texture-related resources`;
        } else {
          results.checks.texturesPresent.status = 'WARN';
          results.checks.texturesPresent.details = 'No texture files detected in requests';
        }
      }
    } else {
      results.checks.sizeConsistency.status = 'SKIP';
      results.checks.sizeConsistency.details = 'Insufficient animations to test';
      results.checks.texturesPresent.status = 'SKIP';
      results.checks.texturesPresent.details = 'Insufficient animations to test';
    }

    // Step 4: Check for 404 errors
    console.log('\nStep 5: Analyzing API errors...');
    const api404Errors = errors.filter(e => e.status === 404 && e.url && e.url.includes('/api/task/'));
    
    if (api404Errors.length === 0) {
      results.checks.noApiErrors.status = 'PASS';
      results.checks.noApiErrors.details = 'No 404 errors on /api/task/ endpoints';
    } else {
      results.checks.noApiErrors.status = 'FAIL';
      results.checks.noApiErrors.details = `Found ${api404Errors.length} 404 error(s)`;
      results.pass = false;
      
      api404Errors.forEach(err => {
        console.log(`   ✗ 404 Error: ${err.url}`);
      });
    }

    // Take final screenshot
    await page.screenshot({ path: '/root/screenshot_final.png', fullPage: true });

    // Generate detailed report
    console.log('\n\n' + '='.repeat(80));
    console.log('VERIFICATION REPORT');
    console.log('='.repeat(80));
    console.log(`\nOverall Status: ${results.pass ? '✓ PASS' : '✗ FAIL'}\n`);
    
    console.log('Detailed Check Results:');
    console.log('-'.repeat(80));
    Object.entries(results.checks).forEach(([check, result]) => {
      const icon = result.status === 'PASS' ? '✓' : 
                   result.status === 'FAIL' ? '✗' : 
                   result.status === 'WARN' ? '⚠' : 
                   result.status === 'SKIP' ? '⊘' : '?';
      const checkName = check.replace(/([A-Z])/g, ' $1').trim();
      console.log(`  ${icon} ${checkName}: ${result.status}`);
      if (result.details) {
        console.log(`     └─ ${result.details}`);
      }
    });

    console.log('\n' + '-'.repeat(80));
    console.log('All Requested URLs:');
    console.log('-'.repeat(80));
    const uniqueUrls = [...new Set(requests.map(r => r.url))];
    const categorizedUrls = {
      api: [],
      models: [],
      textures: [],
      other: []
    };
    
    uniqueUrls.forEach(url => {
      const shortUrl = url.replace('https://autorig.online', '');
      if (url.includes('/api/task/')) {
        categorizedUrls.api.push(shortUrl);
      } else if (url.includes('.glb') || url.includes('.fbx')) {
        categorizedUrls.models.push(shortUrl);
      } else if (url.includes('.jpg') || url.includes('.png') || url.includes('.fbm')) {
        categorizedUrls.textures.push(shortUrl);
      } else {
        categorizedUrls.other.push(shortUrl);
      }
    });
    
    console.log('\nAPI Endpoints:');
    categorizedUrls.api.forEach(url => console.log(`  - ${url}`));
    
    console.log('\nModel Files:');
    categorizedUrls.models.forEach(url => console.log(`  - ${url}`));
    
    if (categorizedUrls.textures.length > 0) {
      console.log('\nTexture Files:');
      categorizedUrls.textures.forEach(url => console.log(`  - ${url}`));
    }
    
    if (categorizedUrls.other.length > 0) {
      console.log('\nOther Resources:');
      categorizedUrls.other.forEach(url => console.log(`  - ${url}`));
    }

    if (errors.length > 0) {
      console.log('\n' + '-'.repeat(80));
      console.log('All Errors Detected:');
      console.log('-'.repeat(80));
      
      const errorsByType = {};
      errors.forEach(err => {
        const type = err.type || 'Unknown';
        if (!errorsByType[type]) errorsByType[type] = [];
        errorsByType[type].push(err);
      });
      
      Object.entries(errorsByType).forEach(([type, errs]) => {
        console.log(`\n${type} (${errs.length}):`);
        errs.forEach(err => {
          if (err.url) {
            const shortUrl = err.url.replace('https://autorig.online', '');
            console.log(`  - ${shortUrl}`);
          } else if (err.message) {
            const shortMsg = err.message.substring(0, 100);
            console.log(`  - ${shortMsg}${err.message.length > 100 ? '...' : ''}`);
          }
        });
      });
    } else {
      console.log('\n✓ No errors detected in console or network');
    }

    console.log('\n' + '-'.repeat(80));
    console.log('Screenshots Saved:');
    console.log('-'.repeat(80));
    console.log('  - /root/screenshot_after_anim_click.png');
    for (let i = 1; i <= 3; i++) {
      console.log(`  - /root/screenshot_anim_${i}.png`);
    }
    console.log('  - /root/screenshot_final.png');
    console.log('  - /root/page_content.html (page source)');
    
    console.log('\n' + '='.repeat(80));

  } catch (error) {
    console.error('\n✗ Test failed with error:', error.message);
    console.error(error.stack);
    await page.screenshot({ path: '/root/screenshot_error.png' });
    results.pass = false;
  } finally {
    await browser.close();
  }
  
  // Exit with appropriate code
  process.exit(results.pass ? 0 : 1);
}

verifyAnimations().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
