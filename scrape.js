// 1. Dependencies & Stealth
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('node:fs/promises'); // Use promise version

puppeteer.use(StealthPlugin());

let currentPage = 0; // Track current page number

// 2. Central Configuration
const CONFIG = {
    MAP_URL: () => {
        const baseUrl = 'https://www.realtor.ca/map#';
        const maxPages = 50;

        currentPage += 1; // Increment page number on each call
        if (currentPage > maxPages) {
            console.log(`Reached max page limit of ${maxPages}. Stopping.`);
            return null; // Signal to stop further processing
        }
        console.log(`Generating URL for page ${currentPage}`);
        
        const params = {
            LatitudeMax: '49.56997',
            LongitudeMax: '-122.40807',
            LatitudeMin: '49.00206',
            LongitudeMin: '-123.71483',
            view: 'list',
            CurrentPage: currentPage.toString(), // Use the currentPage variable
            Sort: '6-D',
            PGeoIds: 'g40_c2b84pnz',
            GeoName: 'Metro Vancouver, BC',
            PropertyTypeGroupID: '1',
            TransactionTypeId: '2',
            PropertySearchTypeId: '0',
            Currency: 'CAD',

        }

        const queryString = new URLSearchParams(params).toString();
        return `${baseUrl}${queryString}`;
    },
    API_URL: 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post',
    OUTPUT_FILE: 'realtor_listings_perfect.json',
    USER_AGENT: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    TIMEOUT_MS: 60000, // For navigation and waiting for response
    SLEEP_BETWEEN_PAGES_MS: 60000, // 60 seconds delay between pages to be respectful to the server
    HEADLESS: false, // true = faster, no UI. false = slower, visual, better for debug/evasion.
    // EXECUTABLE_PATH: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', // <<-- THIS IS NOW UNCOMMENTED
    EXECUTABLE_PATH: '/usr/bin/google-chrome-stable', // Standard path if installed via apt in Docker
    BLOCKED_RESOURCE_TYPES: ['image', 'media', 'font', 'stylesheet', 'other'], // Efficiency: block non essential
    BLOCKED_URL_PATTERNS: ['.css', 'google-analytics', 'googletagmanager', 'doubleclick', 'scorecardresearch', 'youtube', 'intergient'], // Efficiency
};

// 3. Core Logic Function
async function runScraper(browser, mapUrl) {
     console.log("Setting up page...");
     const page = await browser.newPage();
     await page.setUserAgent(CONFIG.USER_AGENT);
     await page.setViewport({ width: 1280, height: 800 }); // Consistent viewport
     await page.setRequestInterception(true);

      // Efficiency: Block resources
      page.on('request', (request) => {
         const url = request.url();
         const resourceType = request.resourceType();
          if (
             CONFIG.BLOCKED_RESOURCE_TYPES.includes(resourceType) ||
             CONFIG.BLOCKED_URL_PATTERNS.some(pattern => url.includes(pattern))
            ) {
              // console.log(`[BLOCK] ${resourceType} ${url.substring(0, 60)}...`);
               request.abort();
          } else {
               // console.log(`[ALLOW] ${resourceType} ${url.substring(0, 60)}...`);
               request.continue();
          }
     });

     console.log(`Waiting for API: ${CONFIG.API_URL}`);
      // Best Practice: Idiomatic Wait
     const apiResponsePromise = page.waitForResponse(
          (response) => response.url() === CONFIG.API_URL && response.request().method() === 'POST',
         { timeout: CONFIG.TIMEOUT_MS }
     );

     console.log(`Navigating to: ${mapUrl}`);
      await page.goto(mapUrl, {
          waitUntil: 'networkidle0', // Efficiency: Wait only for essential network calls
         timeout: CONFIG.TIMEOUT_MS,
      });

       // Optional scroll trigger
      try {
            await page.evaluate(() => window.scrollBy(0, 50));
        } catch (e) { /* ignore scroll errors */ }

       console.log("Awaiting API response...");
       const response = await apiResponsePromise; // Wait for the specific response

       console.log(`API Response Status: ${response.status()}`);
       if (!response.ok()) {
           const text = await response.text();
           throw new Error(`API HTTP Error ${response.status()} ${response.statusText()}. Body: ${text.substring(0, 200)}`);
        }

       const data = await response.json(); // Throws if JSON is invalid
       console.log("API response JSON parsed.");
       
       // Check for stopping conditions
       if (data.Paging && data.Paging.RecordsPerPage === 0) {
           console.log("API returned Paging.RecordsPerPage=0. No more records available.");
           await page.close();
           return { ...data, shouldStop: true };
       }
       
       if (data.ErrorCode && data.ErrorCode.Id === 400) {
           console.log("API returned ErrorCode.Id=400. Stopping due to error.");
           await page.close();
           return { ...data, shouldStop: true };
       }
       
       // Close the page to free memory
       await page.close();
       
       return data;
}

// 4. Data Saving Function
async function saveData(data, filePath, isFirstPage = false) {
     if (!data) {
         console.warn("No data to save.");
         return;
     }
     
     let allData = {};
     
     // Check if file exists and read existing data (unless it's the first page)
     if (!isFirstPage) {
         try {
             const existingContent = await fs.readFile(filePath, 'utf8');
             allData = JSON.parse(existingContent);
         } catch (error) {
             // File doesn't exist or invalid JSON, start with empty structure
             console.log("Starting with new data file or file doesn't exist");
             allData = { Results: [], ...data };
             // Remove Results from data to avoid duplication
             delete allData.Results;
             allData.Results = [];
         }
     } else {
         // First page, start fresh
         allData = { ...data };
         allData.Results = [];
     }
     
     // Append new results to existing ones
     if (data.Results && Array.isArray(data.Results)) {
         allData.Results = allData.Results || [];
         allData.Results.push(...data.Results);
         
         // Update other properties from the latest response
         Object.keys(data).forEach(key => {
             if (key !== 'Results') {
                 allData[key] = data[key];
             }
         });
     }
     
      // Best Practice: Async file IO
      await fs.writeFile(filePath, JSON.stringify(allData, null, 2), 'utf8');
      console.log(`Data saved: ${filePath}`);
      console.log(`New listings in this page: ${data.Results?.length || 0}`);
      console.log(`Total listings so far: ${allData.Results?.length || 0}`);
}

// 5. Main Orchestration Function
async function main() {
     console.log("Script start.");
     let browser;
     try {
        console.log(`Launching browser (Headless: ${CONFIG.HEADLESS})...`);
        // ADDED DEBUG LINE:
        console.log('DEBUG: Launching with executablePath:', CONFIG.EXECUTABLE_PATH); 
        browser = await puppeteer.launch({
             headless: CONFIG.HEADLESS,
            executablePath: CONFIG.EXECUTABLE_PATH, // <<-- THIS IS NOW UNCOMMENTED
            args: ['--no-sandbox', '--disable-setuid-sandbox'],
            ignoreHTTPSErrors: true,
        });
        
        let isFirstPage = true;
        
        // Loop until CONFIG.MAP_URL returns null (when it's a function)
        while (true) {
            const mapUrl = typeof CONFIG.MAP_URL === 'function' ? CONFIG.MAP_URL() : CONFIG.MAP_URL;
            
            // If CONFIG.MAP_URL is a function and returns null, break the loop
            if (mapUrl === null) {
                console.log("CONFIG.MAP_URL returned null. Stopping pagination.");
                break;
            }
            
            // If CONFIG.MAP_URL is not a function, run once and break
            if (typeof CONFIG.MAP_URL !== 'function' && !isFirstPage) {
                console.log("CONFIG.MAP_URL is not a function. Running single page scrape.");
                break;
            }
            
            console.log(`\n--- Processing ${isFirstPage ? 'first' : 'next'} page ---`);
            const data = await runScraper(browser, mapUrl);
            
            // Check if we should stop based on API response conditions
            if (data.shouldStop) {
                console.log("Stopping pagination due to API response conditions.");
                // Save data even if we're stopping (might have some results)
                if (data.Results && data.Results.length > 0) {
                    // Remove the shouldStop flag before saving
                    const { shouldStop, ...dataToSave } = data;
                    await saveData(dataToSave, CONFIG.OUTPUT_FILE, isFirstPage);
                }
                break;
            }
            
            await saveData(data, CONFIG.OUTPUT_FILE, isFirstPage);
            
            isFirstPage = false;
            
            // If CONFIG.MAP_URL is not a function, break after first iteration
            if (typeof CONFIG.MAP_URL !== 'function') {
                break;
            }
            
            // Add a small delay between pages to be respectful to the server
            console.log(`Waiting ${CONFIG.SLEEP_BETWEEN_PAGES_MS / 1000} seconds before next page...`);
            await new Promise(resolve => setTimeout(resolve, CONFIG.SLEEP_BETWEEN_PAGES_MS));
        }
        
        console.log("Script success.");

    } catch (error) {
        console.error("Script Error:", error.message || error);
    } finally {
        if (browser) {
            console.log("Closing browser.");
            await browser.close();
        }
         console.log("Script end.");
    }
}

// Execute
main().catch(err => {
     console.error("Unhandled Main Error:", err);
     process.exit(1); // Exit with error code
});