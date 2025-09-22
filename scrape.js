// 1. Dependencies & Stealth
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('node:fs/promises'); // Use promise version
const geoNames = require('./config/geoNames');
const transactionTypeIds = [
    1, // For sale
    5, // Sold
]

puppeteer.use(StealthPlugin());

// State tracking for nested loops
let currentTransactionIndex = 0;
let currentGeoIndex = 0;
let currentPage = 0;

// 2. Central Configuration
const CONFIG = {
    MAP_URL: () => {
        const baseUrl = 'https://www.realtor.ca/map#';
        const maxPages = 50;

        // Increment page
        currentPage += 1;
        
        // Check if we need to move to next geo area
        if (currentPage > maxPages) {
            currentPage = 1;
            currentGeoIndex += 1;
            
            // Check if we need to move to next transaction type
            if (currentGeoIndex >= geoNames.length) {
                currentGeoIndex = 0;
                currentTransactionIndex += 1;
                
                // Check if we're done with all combinations
                if (currentTransactionIndex >= transactionTypeIds.length) {
                    console.log(`Completed all combinations. Stopping.`);
                    return null; // Signal to stop further processing
                }
            }
        }
        
        const currentTransactionId = transactionTypeIds[currentTransactionIndex];
        const currentGeo = geoNames[currentGeoIndex];
        
        console.log(`Generating URL for Transaction: ${currentTransactionId}, Geo: ${currentGeo.GeoName}, Page: ${currentPage}`);
        
        const params = {
            ZoomLevel: '11',
            Center: currentGeo.Center,
            LatitudeMax: currentGeo.LatitudeMax,
            LongitudeMax: currentGeo.LongitudeMax,
            LatitudeMin: currentGeo.LatitudeMin,
            LongitudeMin: currentGeo.LongitudeMin,
            view: 'list',
            ...( currentPage > 1 ? { CurrentPage: currentPage.toString()} : {} ),
            Sort: '6-D',
            PGeoIds: currentGeo.PGeoIds,
            GeoName: currentGeo.GeoName,
            PropertyTypeGroupID: '1',
            TransactionTypeId: currentTransactionId.toString(),
            PropertySearchTypeId: '0',
            Currency: 'CAD',
        }

        const queryString = new URLSearchParams(params).toString();
        return `${baseUrl}${queryString}`;
    },
    API_URL: 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post',
    OUTPUT_FILE: './data/realtor_listings_perfect.json',
    BACKUP_DIR: './data/backups',
    getBackupFileName: () => {
        const now = new Date();
        const yyyy = now.getFullYear();
        const mm = String(now.getMonth() + 1).padStart(2, '0');
        const dd = String(now.getDate()).padStart(2, '0');
        const hh = String(now.getHours()).padStart(2, '0');
        const ii = String(now.getMinutes()).padStart(2, '0');
        return `realtor_listings_perfect.${yyyy}-${mm}-${dd}-${hh}-${ii}.json`;
    },
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
            await page.evaluate(() => window.scrollBy(0, 100));
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
     
     // Always try to read existing data (removed isFirstPage check)
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
     
     // Append new results to existing ones
     if (data.Results && Array.isArray(data.Results)) {
         allData.Results = allData.Results || [];
         
         // Create a Map for efficient deduplication by Id
         const resultsMap = new Map();
         
         // First, add all existing results to the map
         allData.Results.forEach(result => {
             if (result.Id) {
                 resultsMap.set(result.Id, result);
             }
         });
         
         // Then add/update with new results (this will overwrite duplicates with newer data)
         data.Results.forEach(result => {
             if (result.Id) {
                 resultsMap.set(result.Id, result);
             }
         });
         
         // Convert map back to array
         allData.Results = Array.from(resultsMap.values());
         
         console.log(`New listings in this page: ${data.Results?.length || 0}`);
         console.log(`Unique listings after deduplication: ${allData.Results?.length || 0}`);
         
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
      console.log(`Total unique listings so far: ${allData.Results?.length || 0}`);
}

// 5. Backup Function
async function createBackup(originalFile, backupFileName) {
    try {
        // Ensure backup directory exists
        await fs.mkdir(CONFIG.BACKUP_DIR, { recursive: true });
        
        const backupFilePath = `${CONFIG.BACKUP_DIR}/${backupFileName}`;
        const existingContent = await fs.readFile(originalFile, 'utf8');
        await fs.writeFile(backupFilePath, existingContent, 'utf8');
        console.log(`Backup created: ${backupFilePath}`);
        return true;
    } catch (error) {
        console.log(`No existing file to backup or backup failed: ${error.message}`);
        return false;
    }
}

// 6. Main Orchestration Function
async function main() {
     console.log("Script start.");
     let browser;
     
     // Create backup of existing file before starting
     const backupFileName = CONFIG.getBackupFileName();
     await createBackup(CONFIG.OUTPUT_FILE, backupFileName);
     
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
                    await saveData(dataToSave, CONFIG.OUTPUT_FILE);
                }
                break;
            }
            
            await saveData(data, CONFIG.OUTPUT_FILE);
            
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