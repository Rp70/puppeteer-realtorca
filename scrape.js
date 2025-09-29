// 1. Dependencies & Stealth
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('node:fs/promises'); // Use promise version
const path = require('path');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const geoNames = require('./config/geoNames');

const transactionTypeIds = [
    2, // For sale
    5, // Sold
]

puppeteer.use(StealthPlugin());

// Parse command line arguments
const args = process.argv.slice(2);

if (args.includes('--help') || args.includes('-h')) {
    console.log('Usage: node scrape.js');
    console.log('');
    console.log('The script runs in integrated mode:');
    console.log('  1. Scrapes each page of listings from realtor.ca API');
    console.log('  2. Immediately scrapes detailed property information for that page');
    console.log('  3. Merges and saves both listings and details together per page');
    console.log('');
    console.log('Options:');
    console.log('  --help, -h       Show this help message');
    process.exit(0);
}

// State tracking for nested loops
let currentTransactionIndex = 0;
let currentGeoIndex = 0;
let currentPage = 0;

// Error retry tracking
let consecutiveErrorCount = 0;
const MAX_CONSECUTIVE_ERRORS = 3;

// 2. Central Configuration
const CONFIG = {
    MAP_URL: () => {
        const baseUrl = 'https://www.realtor.ca/map#';
        const maxPages = 10;

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
            
            // Signal that we need to delay before the next geo area
            return { shouldDelayForGeo: true };
        }
        
        const currentTransactionId = transactionTypeIds[currentTransactionIndex];
        const currentGeo = geoNames[currentGeoIndex];
        
        console.log(`Generating URL for Transaction: ${currentTransactionId}, Geo: ${currentGeo.GeoName}, Page: ${currentPage}`);
        
        const params = {
            // ZoomLevel: '11', // DO NOT enablle this until we understand its impact
            // Center: currentGeo.Center, // DO NOT enablle this until we understand its impact
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
    DETAIL_FILE: './tmp/detail.html',
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
    // --- Retry Configuration ---
    MAX_RETRY_ATTEMPTS: 2,     // 1 initial attempt + 2 retries = 3 total attempts
    RETRY_DELAY_MS: 5000,      // 5-second delay between retry attempts
    getSleepBetweenPages: () => {
        // Randomize between 60-80 seconds (60000-80000 ms)
        const minMs = 60000;
        const maxMs = 80000;
        return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    },
    getSleepBetweenGeos: () => {
        // Randomize between 1111-3333 seconds (1111000-3333000 ms)
        const minMs = 1111000;
        const maxMs = 3333000;
        return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    },
    HEADLESS: false, // true = faster, no UI. false = slower, visual, better for debug/evasion.
    // EXECUTABLE_PATH: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', // <<-- THIS IS NOW UNCOMMENTED
    EXECUTABLE_PATH: '/usr/bin/google-chrome-stable', // Standard path if installed via apt in Docker
    BLOCKED_RESOURCE_TYPES: ['image', 'media', 'font', 'stylesheet', 'other'], // Efficiency: block non essential
    BLOCKED_URL_PATTERNS: ['.css', 'google-analytics', 'googletagmanager', 'doubleclick', 'scorecardresearch', 'youtube', 'intergient'], // Efficiency
    RESUME: true, // Whether to resume from last state in data/scraper.json
    STATE_FILE: './data/scraper.json', // File to store scraper state
};

// State Management Functions
async function loadScraperState() {
    if (!CONFIG.RESUME) {
        console.log("Resume disabled. Starting from beginning.");
        return { currentTransactionIndex: 0, currentGeoIndex: 0, currentPage: 0 };
    }
    
    try {
        const stateContent = await fs.readFile(CONFIG.STATE_FILE, 'utf8');
        const state = JSON.parse(stateContent);
        console.log(`Resuming from: Transaction ${state.currentTransactionIndex}, Geo ${state.currentGeoIndex}, Page ${state.currentPage}`);
        return state;
    } catch (error) {
        console.log("No existing state file found. Starting from beginning.");
        return { currentTransactionIndex: 0, currentGeoIndex: 0, currentPage: 0 };
    }
}

async function saveScraperState() {
    try {
        // Ensure data directory exists
        await fs.mkdir('./data', { recursive: true });
        
        const state = {
            currentTransactionIndex,
            currentGeoIndex,
            currentPage,
            lastUpdated: new Date().toISOString()
        };
        
        await fs.writeFile(CONFIG.STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
        const currentGeoName = geoNames[currentGeoIndex] ? geoNames[currentGeoIndex].GeoName : 'N/A';
        console.log(`State saved: Transaction Index ${currentTransactionIndex}, Geo Index: ${currentGeoIndex} (${currentGeoName}), Page Index: ${currentPage}`);
    } catch (error) {
        console.warn(`Failed to save scraper state: ${error.message}`);
    }
}

async function resetScraperState() {
    try {
        // Ensure data directory exists
        await fs.mkdir('./data', { recursive: true });
        
        const initialState = {
            currentTransactionIndex: 0,
            currentGeoIndex: 0,
            currentPage: 0,
            lastUpdated: new Date().toISOString()
        };
        
        await fs.writeFile(CONFIG.STATE_FILE, JSON.stringify(initialState, null, 2), 'utf8');
        console.log("State file reset to initial values for next cycle.");
    } catch (error) {
        console.warn(`Failed to reset scraper state: ${error.message}`);
    }
}



// Error Handling Functions
async function handleServerError(errorType, details = '') {
    consecutiveErrorCount++;
    console.warn(`Server error detected (${errorType}): ${details}`);
    console.warn(`Consecutive error count: ${consecutiveErrorCount}/${MAX_CONSECUTIVE_ERRORS}`);
    
    if (consecutiveErrorCount >= MAX_CONSECUTIVE_ERRORS) {
        console.error(`Maximum consecutive errors (${MAX_CONSECUTIVE_ERRORS}) reached. Exiting script.`);
        process.exit(1);
    }
    
    const sleepMs = CONFIG.getSleepBetweenGeos();
    console.log(`Sleeping for ${Math.round(sleepMs / 1000)} seconds due to server error...`);
    await new Promise(resolve => setTimeout(resolve, sleepMs));
    console.log('Waking up from error sleep, retrying...');
}

function resetErrorCount() {
    if (consecutiveErrorCount > 0) {
        console.log('Server response successful, resetting error count.');
        consecutiveErrorCount = 0;
    }
}

function isServerError(statusCode) {
    return statusCode >= 400 && statusCode <= 599;
}

// 3. Core Logic Function with Retry Pattern
async function runScraper(browser, mapUrl) {
    let page;
    let lastError = null;

    for (let attempt = 1; attempt <= CONFIG.MAX_RETRY_ATTEMPTS + 1; attempt++) {
        console.log(`--- Scraping Attempt #${attempt} of ${CONFIG.MAX_RETRY_ATTEMPTS + 1} ---`);
        try {
            if (page && !page.isClosed()) {
                console.log("Closing previous page before new attempt...");
                await page.close();
            }
            
            console.log("Setting up new page for attempt...");
            page = await browser.newPage();
            
            // Add page error listeners for debugging
            page.on('pageerror', function(err) {
                const theTempValue = err.toString();
                console.log(`[PAGE JS ERROR on attempt #${attempt}]: ${theTempValue}`);
            });
            page.on('error', function(err) { 
                const theTempValue = err.toString();
                console.log(`[PAGE CRASH/OTHER ERROR on attempt #${attempt}]: ${theTempValue}`);
            });

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
                    request.abort();
                } else {
                    request.continue();
                }
            });

            console.log(`Waiting for API: ${CONFIG.API_URL}`);
            // Best Practice: Idiomatic Wait
            const apiResponsePromise = page.waitForResponse(
                (response) => response.url() === CONFIG.API_URL && response.request().method() === 'POST',
                { timeout: CONFIG.TIMEOUT_MS }
            );

            console.log(`Navigating to: ${mapUrl} (Timeout: ${CONFIG.TIMEOUT_MS / 1000}s)`);
            await page.goto(mapUrl, {
                waitUntil: 'networkidle0', // Efficiency: Wait only for essential network calls
                timeout: CONFIG.TIMEOUT_MS,
            });
            console.log("Navigation complete for this attempt.");

            // Optional scroll trigger
            try {
                await page.evaluate(() => window.scrollBy(0, 100));
                console.log("Page scrolled.");
            } catch (e) { 
                console.warn("Scroll failed, continuing...");
            }

            console.log(`Awaiting API response (Timeout: ${CONFIG.TIMEOUT_MS / 1000}s)...`);
            const response = await apiResponsePromise; // Wait for the specific response

            console.log(`API Response Status: ${response.status()}`);
            if (!response.ok()) {
                const text = await response.text();
                const errorMsg = `API HTTP Error ${response.status()} ${response.statusText()}. Body: ${text.substring(0, 200)}`;
                console.error(errorMsg);
                
                // Check if it's a server error (4xx or 5xx)
                if (isServerError(response.status())) {
                    if (page && !page.isClosed()) await page.close();
                    await handleServerError(`API ${response.status()}`, `${response.statusText()}. Body: ${text.substring(0, 200)}`);
                    return { shouldRetry: true };
                }
                
                throw new Error(errorMsg);
            }

            const data = await response.json(); // Throws if JSON is invalid
            console.log(`API response JSON parsed successfully on attempt #${attempt}`);
            
            // Reset error count on successful API response
            resetErrorCount();
            
            // Check for stopping conditions
            if (data.Paging && data.Paging.RecordsPerPage === 0) {
                console.log("API returned Paging.RecordsPerPage=0. No more records available.");
                if (page && !page.isClosed()) await page.close();
                return { ...data, shouldStop: true };
            }
            
            if (data.ErrorCode && data.ErrorCode.Id === 400) {
                console.log("API returned ErrorCode.Id=400. Stopping due to error.");
                if (page && !page.isClosed()) await page.close();
                return { ...data, shouldStop: true };
            }
            
            // Close the page to free memory
            if (page && !page.isClosed()) await page.close();
            
            return data;

        } catch (error) {
            lastError = error; 
            console.error(`Attempt #${attempt} failed: ${error.message}`);
            if (error.name === 'TimeoutError') {
                console.error(error.stack);
            }
            
            if (attempt > CONFIG.MAX_RETRY_ATTEMPTS) {
                console.error("Max retry attempts reached for scraping. Failing operation.");
                if (page && !page.isClosed()) await page.close();
                throw lastError; 
            }
            
            console.log(`Preparing for next attempt after ${CONFIG.RETRY_DELAY_MS / 1000} seconds delay...`);
            await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY_MS));
        }
    }
    
    if (page && !page.isClosed()) await page.close();
    throw lastError || new Error("Scraping failed after all attempts; unknown state.");
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

// 6. Image Download Functions (HTTP-based but using browser's User-Agent)
async function downloadImage(url, filePath, userAgent) {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(url);
        const client = parsedUrl.protocol === 'https:' ? https : http;
        
        const options = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port,
            path: parsedUrl.pathname + parsedUrl.search,
            headers: {
                'User-Agent': userAgent,
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Referer': 'https://www.realtor.ca/',
                'Host': parsedUrl.hostname
            }
        };
        
        const request = client.get(options, (response) => {
            // Handle redirects
            if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
                downloadImage(response.headers.location, filePath, userAgent)
                    .then(resolve)
                    .catch(reject);
                return;
            }
            
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            const fileStream = require('fs').createWriteStream(filePath);
            response.pipe(fileStream);
            
            fileStream.on('error', reject);
            fileStream.on('finish', () => {
                fileStream.close();
                resolve(filePath);
            });
        });
        
        request.on('error', reject);
        request.setTimeout(30000, () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

// Helper function to check if images need to be updated based on PhotoChangeDateUTC
async function shouldUpdateImages(propertyId, newPhotoChangeDateUTC) {
    if (!newPhotoChangeDateUTC) {
        console.log(`No PhotoChangeDateUTC provided for property ${propertyId}, skipping image download`);
        return false;
    }
    
    try {
        // Read existing listings to find current PhotoChangeDateUTC
        const existingContent = await fs.readFile(CONFIG.OUTPUT_FILE, 'utf8');
        const listingsData = JSON.parse(existingContent);
        
        if (!listingsData.Results || !Array.isArray(listingsData.Results)) {
            console.log(`No existing listings found, will download images for property ${propertyId}`);
            return true;
        }
        
        // Find the existing listing for this property
        const existingListing = listingsData.Results.find(item => item.Id === propertyId);
        
        if (!existingListing) {
            console.log(`Property ${propertyId} not found in existing listings, will download images`);
            return true;
        }
        
        const existingPhotoChangeDateUTC = existingListing.PhotoChangeDateUTC;
        
        if (!existingPhotoChangeDateUTC) {
            console.log(`No existing PhotoChangeDateUTC for property ${propertyId}, will download images`);
            return true;
        }
        
        // Parse dates and compare
        const existingDate = new Date(existingPhotoChangeDateUTC);
        const newDate = new Date(newPhotoChangeDateUTC);
        
        if (newDate > existingDate) {
            console.log(`PhotoChangeDateUTC is newer for property ${propertyId} (${newPhotoChangeDateUTC} > ${existingPhotoChangeDateUTC}), will update images`);
            return true;
        } else {
            console.log(`PhotoChangeDateUTC is same or older for property ${propertyId} (${newPhotoChangeDateUTC} <= ${existingPhotoChangeDateUTC}), skipping image download`);
            return false;
        }
        
    } catch (error) {
        console.warn(`Error checking PhotoChangeDateUTC for property ${propertyId}: ${error.message}, will download images`);
        return true;
    }
}

// Helper function to delete existing images for a property
async function deleteExistingImages(propertyId) {
    const imagesDir = `./data/properties/${propertyId}/images`;
    
    try {
        // Check if images directory exists
        await fs.access(imagesDir);
        
        // Read all files in the images directory
        const files = await fs.readdir(imagesDir);
        
        // Delete all image files
        for (const file of files) {
            const filePath = path.join(imagesDir, file);
            await fs.unlink(filePath);
            console.log(`Deleted old image: ${file}`);
        }
        
        console.log(`Deleted ${files.length} old images for property ${propertyId}`);
        
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log(`No existing images directory for property ${propertyId}`);
        } else {
            console.warn(`Error deleting existing images for property ${propertyId}: ${error.message}`);
        }
    }
}

async function savePropertyImages(propertyId, images, userAgent, photoChangeDateUTC = null) {
    if (!propertyId || !images || images.length === 0) {
        return [];
    }
    
    // Check if we need to update images based on PhotoChangeDateUTC
    const needsUpdate = await shouldUpdateImages(propertyId, photoChangeDateUTC);
    
    if (!needsUpdate) {
        // Return existing images info if they exist
        const imagesDir = `./data/properties/${propertyId}/images`;
        try {
            const files = await fs.readdir(imagesDir);
            const existingImages = files.map((filename, index) => ({
                url: `local://${path.join(imagesDir, filename)}`,
                type: filename.startsWith('hero') ? 'hero' : filename.startsWith('grid') ? 'grid' : 'gallery',
                index: index,
                localPath: path.join(imagesDir, filename),
                filename: filename
            }));
            console.log(`Using existing ${existingImages.length} images for property ${propertyId}`);
            return existingImages;
        } catch (error) {
            // If we can't read existing images, continue with download
            console.log(`Could not read existing images for property ${propertyId}, will download new ones`);
        }
    }
    
    const propertyDir = `./data/properties/${propertyId}`;
    const imagesDir = `${propertyDir}/images`;
    
    // Delete existing images if we're updating
    if (needsUpdate) {
        await deleteExistingImages(propertyId);
    }
    
    // Create directories
    await fs.mkdir(imagesDir, { recursive: true });
    
    const savedImages = [];
    
    for (let i = 0; i < images.length; i++) {
        const image = images[i];
        try {
            // Extract file extension from URL
            const urlPath = new URL(image.url).pathname;
            const extension = path.extname(urlPath) || '.jpg';
            
            // Generate filename based on type and index
            let filename;
            if (image.type === 'hero') {
                filename = `hero${extension}`;
            } else if (image.type === 'grid') {
                filename = `grid_${(image.index || i).toString().padStart(2, '0')}${extension}`;
            } else if (image.type === 'gallery') {
                filename = `gallery_${(image.index || i).toString().padStart(2, '0')}${extension}`;
            } else {
                filename = `image_${i.toString().padStart(2, '0')}${extension}`;
            }
            
            const filePath = path.join(imagesDir, filename);
            
            console.log(`Downloading ${image.type} image: ${image.url}`);
            await downloadImage(image.url, filePath, userAgent);
            
            savedImages.push({
                ...image,
                localPath: filePath,
                filename: filename
            });
            
            console.log(`Saved: ${filename}`);
            
        } catch (error) {
            console.warn(`Failed to download image ${image.url}: ${error.message}`);
            
            // Check if it's an HTTP error (4xx or 5xx)
            if (error.message.includes('HTTP 4') || error.message.includes('HTTP 5')) {
                console.warn(`HTTP error downloading image: ${error.message}`);
                // Don't throw here, but the caller will check if no images were saved
            }
            // Continue with other images even if one fails
        }
    }
    
    return savedImages;
}

// 7. Detail Scraping Functions
async function scrapePropertyDetail(browser, relativeUrl) {
    const fullUrl = `https://www.realtor.ca${relativeUrl}`;
    console.log(`Scraping detail page: ${fullUrl}`);
    
    const page = await browser.newPage();
    await page.setUserAgent(CONFIG.USER_AGENT);
    await page.setViewport({ width: 1280, height: 800 });
    await page.setRequestInterception(true);

    // Apply selective blocking - allow property images but block other unnecessary resources
    page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();
        
        // Allow property images from realtor.ca CDN
        if (resourceType === 'image' && url.includes('realtor.ca')) {
            // console.log(`[ALLOW PROPERTY IMAGE] ${url.substring(0, 80)}...`);
            request.continue();
            return;
        }
        
        // Block other unnecessary resources
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
    
    try {
        const response = await page.goto(fullUrl, {
            waitUntil: 'networkidle0',
            timeout: CONFIG.TIMEOUT_MS
        });
        
        // Check for HTTP errors
        if (response && isServerError(response.status())) {
            await page.close();
            throw new Error(`HTTP ${response.status()}: ${response.statusText()}`);
        }
        
        // Get the full HTML content
        const htmlContent = await page.content();
        
        // Ensure tmp directory exists before saving detail file
        await fs.mkdir(path.dirname(CONFIG.DETAIL_FILE), { recursive: true });
        
        // Save to detail file
        await fs.writeFile(CONFIG.DETAIL_FILE, htmlContent, 'utf8');
        
        // Extract property information from the page
        const propertyDetails = await page.evaluate(() => {
            const extractText = (selector) => {
                const element = document.querySelector(selector);
                return element ? element.textContent.trim() : null;
            };
            
            const extractAttribute = (selector, attribute) => {
                const element = document.querySelector(selector);
                return element ? element.getAttribute(attribute) : null;
            };
            
            const extractMultiple = (selector) => {
                const elements = document.querySelectorAll(selector);
                return Array.from(elements).map(el => el.textContent.trim()).filter(text => text.length > 0);
            };
            
            // Extract price
            const price = extractText('#listingPriceValue') || extractText('[data-value-cad]') || extractText('.listingPrice');
            
            // Extract address
            const address = extractText('#listingAddress') || extractText('h1');
            
            // Extract MLS number
            const mlsNumber = extractText('#MLNumberVal') || extractText('#mlsNumber');
            
            // Extract property ID from URL or data attributes
            const propertyId = window.location.pathname.match(/\/(\d+)\//)?.[1] || extractAttribute('[data-property-id]', 'data-property-id');
            
            // Extract from dataLayer if available
            let dataLayerInfo = {};
            if (typeof window.dataLayer !== 'undefined' && window.dataLayer.length > 0) {
                const propertyData = window.dataLayer.find(layer => layer.property);
                if (propertyData && propertyData.property) {
                    dataLayerInfo = propertyData.property;
                }
            }
            
            // Extract only additional detail information not present in realtor_listings_perfect.json
            // This avoids duplication of data already available in the main listings file
            const details = {
                // Primary identifier to link with main listings data
                Id: propertyId,
                
                // Only extracted details unique to detail scraping
                extractedDetails: {
                    yearBuilt: extractText('.yearBuiltValue') || extractText('[data-year-built]'),
                    neighbourhood: dataLayerInfo.neighbourhood,
                    buildingStyle: dataLayerInfo.buildingStyle,
                    propertyDetails: {},
                    features: [],
                    images: [],
                    agent: {
                        name: extractText('.realtorCardName') || extractText('.agentName'),
                        phone: extractText('.realtorCardPhone') || extractText('.agentPhone'),
                        email: extractText('.realtorCardEmail') || extractText('.agentEmail'),
                        company: extractText('.realtorCardOfficeName') || extractText('.agentCompany')
                    },
                    scrapedAt: new Date().toISOString(),
                    sourceUrl: window.location.href
                }
            };
            
            // Extract property details from multiple possible table structures
            const detailSelectors = [
                '.propertyDetailsTable tr',
                '.property-details-table tr',
                '.listingDetailsTable tr',
                '.propertyDetailsSectionContentSubCon',
                '.propertyDetailsValueSubSectionCon'
            ];
            
            detailSelectors.forEach(selector => {
                const rows = document.querySelectorAll(selector);
                rows.forEach(row => {
                    let label, value;
                    
                    if (selector.includes('SubCon')) {
                        // Handle property details sections
                        label = row.querySelector('.propertyDetailsSectionContentLabel, .propertyDetailsValueSubSectionHeader');
                        value = row.querySelector('.propertyDetailsSectionContentValue, .propertyDetailsValueSubSectionValue');
                    } else {
                        // Handle table rows
                        label = row.querySelector('td:first-child, th:first-child, .label');
                        value = row.querySelector('td:last-child, td:nth-child(2), .value');
                    }
                    
                    if (label && value && label.textContent && value.textContent) {
                        const key = label.textContent.trim().replace(':', '').replace(/\s+/g, '_');
                        const val = value.textContent.trim();
                        if (key && val && key !== val) {
                            details.extractedDetails.propertyDetails[key] = val;
                        }
                    }
                });
            });
            
            // Extract features from multiple possible selectors
            const featureSelectors = [
                '.featureList li',
                '.propertyFeatures li',
                '.amenities li',
                '.features li',
                '.listingFeatures li'
            ];
            
            featureSelectors.forEach(selector => {
                const features = extractMultiple(selector);
                details.extractedDetails.features = details.extractedDetails.features.concat(features);
            });
            
            // Remove duplicates from features
            details.extractedDetails.features = [...new Set(details.extractedDetails.features)];
            
            // Extract images with specific types and roles
            const imageTypes = {
                hero: [],
                grid: [],
                gallery: []
            };
            
            // Extract hero image
            const heroImage = document.querySelector('img#heroImage');
            if (heroImage) {
                const src = heroImage.src || heroImage.getAttribute('data-src') || heroImage.getAttribute('data-original');
                if (src && src.includes('realtor.ca')) {
                    imageTypes.hero.push({
                        url: src,
                        type: 'hero'
                    });
                }
            }
            
            // Extract grid view listing images (multiple images possible)
            const gridImages = document.querySelectorAll('img.topGridViewListingImage');
            gridImages.forEach((gridImage, index) => {
                const src = gridImage.src || gridImage.getAttribute('data-src') || gridImage.getAttribute('data-original');
                if (src && src.includes('realtor.ca')) {
                    imageTypes.grid.push({
                        url: src,
                        type: 'grid',
                        index: index
                    });
                }
            });
            
            // Extract gallery images from sidebar
            const galleryImages = document.querySelectorAll('img.gridViewListingImage');
            galleryImages.forEach((img, index) => {
                const src = img.src || img.getAttribute('data-src') || img.getAttribute('data-original');
                if (src && src.includes('realtor.ca')) {
                    // Check if this image is already a hero or grid image
                    const isHeroImage = imageTypes.hero.some(heroImg => heroImg.url === src);
                    const isGridImage = imageTypes.grid.some(gridImg => gridImg.url === src);
                    
                    // Only add to gallery if it's not already a hero or grid image
                    if (!isHeroImage && !isGridImage) {
                        imageTypes.gallery.push({
                            url: src,
                            type: 'gallery',
                            index: index
                        });
                    }
                }
            });
            
            // Combine all images into a single array with metadata
            details.extractedDetails.images = [
                ...imageTypes.hero,
                ...imageTypes.grid,
                ...imageTypes.gallery
            ];
            
            // Also extract any remaining images as fallback
            const fallbackSelectors = [
                '.propertyPhoto img',
                '.listing-photo img',
                '.gallery img:not([class*="imageGallerySidebarPhoto"])',
                '.listingPhoto img',
                '.photoGallery img'
            ];
            
            fallbackSelectors.forEach(selector => {
                const imageElements = document.querySelectorAll(selector);
                imageElements.forEach((img, index) => {
                    const src = img.src || img.getAttribute('data-src') || img.getAttribute('data-original');
                    if (src && !src.includes('placeholder') && !src.includes('icon') && src.includes('realtor.ca')) {
                        // Check if this image is already in our typed images
                        const alreadyExists = details.extractedDetails.images.some(existingImg => existingImg.url === src);
                        if (!alreadyExists) {
                            details.extractedDetails.images.push({
                                url: src,
                                type: 'other',
                                selector: selector,
                                index: index
                            });
                        }
                    }
                });
            });
            
            return details;
        });
        
        // Return both page and details so images can be captured before page closes
        return { page, details: propertyDetails };
        
    } catch (error) {
        console.error(`Error scraping ${fullUrl}: ${error.message}`);
        await page.close();
        return null;
    }
}

// Helper function to run detail scraping for a single page of listings
async function runDetailScrapingForPage(pageResults, browser, pageData) {
    if (!pageResults || !Array.isArray(pageResults) || pageResults.length === 0) {
        console.log("No listings to process for detail scraping");
        return;
    }
    
    // Filter items with RelativeDetailsURL
    const itemsWithDetails = pageResults.filter(item => item.RelativeDetailsURL);
    console.log(`Found ${itemsWithDetails.length} listings with detail URLs in current page`);
    
    if (itemsWithDetails.length === 0) {
        console.log("No listings with detail URLs found in current page");
        return;
    }
    
    console.log(`Processing ${itemsWithDetails.length} items for details in current page using shared browser instance`);
    
    try {
        for (let i = 0; i < itemsWithDetails.length; i++) {
            const item = itemsWithDetails[i];
            const propertyId = item.RelativeDetailsURL?.match(/\/(\d+)\//)?.[1];
            
            console.log(`Processing detail ${i + 1}/${itemsWithDetails.length}: ${item.RelativeDetailsURL} (Property ID: ${propertyId})`);
            
            let result;
            try {
                result = await scrapePropertyDetail(browser, item.RelativeDetailsURL);
            } catch (error) {
                // Check if it's an HTTP error
                if (error.message.includes('HTTP 4') || error.message.includes('HTTP 5')) {
                    console.warn(`HTTP error for property ${propertyId}: ${error.message}`);
                    await handleServerError('Detail page HTTP error', error.message);
                    i--; // Retry the same property
                    continue;
                }
                // Re-throw non-HTTP errors
                throw error;
            }
            
            if (result && result.details) {
                const { page, details } = result;
                
                // Download and save images with proper headers to avoid detection
                console.log(`Found ${details.extractedDetails.images.length} images for property ${details.Id}`);
                
                // Check for no images condition
                if (details.extractedDetails.images.length === 0) {
                    console.warn(`No images found for property ${details.Id}`);
                    await page.close();
                    await handleServerError('No images', `Property ${details.Id} has no images`);
                    i--; // Retry the same property
                    continue;
                }
                
                const savedImages = await savePropertyImages(details.Id, details.extractedDetails.images, CONFIG.USER_AGENT, item.PhotoChangeDateUTC);
                
                // Check if image downloading failed (no images saved despite having image URLs)
                if (savedImages.length === 0 && details.extractedDetails.images.length > 0) {
                    console.warn(`Failed to download any images for property ${details.Id}`);
                    await page.close();
                    await handleServerError('Image download failed', `Property ${details.Id} image downloads failed`);
                    i--; // Retry the same property
                    continue;
                }
                
                // Reset error count on successful processing
                resetErrorCount();
                
                // Update the details with saved image information
                details.extractedDetails.images = savedImages;
                details.extractedDetails.imagesSaved = savedImages.length;
                details.extractedDetails.imagesDirectory = `./data/properties/${details.Id}/images/`;
                
                // Close the page after processing
                await page.close();
                
                // DIRECTLY ADD extractedDetails to the original listing item
                item.extractedDetails = details.extractedDetails;
                
                // Add other detail properties with X_ prefix if they don't exist in listing
                Object.keys(details).forEach(key => {
                    if (key !== 'Id' && key !== 'extractedDetails') {
                        const prefixedKey = `X_${key}`;
                        if (!item.hasOwnProperty(key)) {
                            item[prefixedKey] = details[key];
                        }
                        // If key exists, we ignore it to avoid overwriting original listing data
                    }
                });
                
                console.log(`Detail processed and added directly to listing item: ${i + 1}/${itemsWithDetails.length} (${details.extractedDetails.imagesSaved} images saved)`);
                
                // SAVE DATA AFTER EACH PROPERTY DETAIL IS SUCCESSFULLY PROCESSED
                console.log(`Saving data after processing property ${propertyId} details...`);
                await saveData(pageData, CONFIG.OUTPUT_FILE);
                console.log(`Data saved after property ${propertyId} detail processing`);
            }
            
            // Add delay between requests
            const sleepMs = CONFIG.getSleepBetweenPages();
            console.log(`Waiting ${sleepMs / 1000} seconds before next detail page...`);
            await new Promise(resolve => setTimeout(resolve, sleepMs));
        }
        
        console.log(`Details processed and saved for all ${itemsWithDetails.length} listing items in current page`);
        
    } catch (error) {
        console.error(`Detail scraping error for page: ${error.message}`);
        // Don't close browser here since it's shared - let the main function handle it
    }
}

// 7. Main Orchestration Function
async function main() {
     console.log("Script start - Integrated listings and details scraping (per-page detail processing).");
     
     let browser;
     
     console.log("\n=== INTEGRATED LISTINGS AND DETAILS SCRAPING ===");
     
     // Load scraper state if resuming
     const savedState = await loadScraperState();
     currentTransactionIndex = savedState.currentTransactionIndex;
     currentGeoIndex = savedState.currentGeoIndex;
     currentPage = savedState.currentPage;
     
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
            const mapUrlResult = typeof CONFIG.MAP_URL === 'function' ? CONFIG.MAP_URL() : CONFIG.MAP_URL;
            
            // If CONFIG.MAP_URL is a function and returns null, break the loop
            if (mapUrlResult === null) {
                console.log("CONFIG.MAP_URL returned null. Stopping pagination.");
                break;
            }
            
            // Check if we need to delay for geo change
            if (mapUrlResult && typeof mapUrlResult === 'object' && mapUrlResult.shouldDelayForGeo) {
                const geoSleepMs = CONFIG.getSleepBetweenGeos();
                const currentGeo = geoNames[currentGeoIndex];
                const currentTransactionId = transactionTypeIds[currentTransactionIndex];
                
                console.log(`\n=== SWITCHING TO NEW GEO AREA ===`);
                console.log(`Moving to Transaction: ${currentTransactionId}, Geo: ${currentGeo.GeoName}`);
                console.log(`Waiting ${Math.round(geoSleepMs / 1000)} seconds before starting new geo area...`);
                
                await new Promise(resolve => setTimeout(resolve, geoSleepMs));
                
                console.log(`Delay completed. Starting geo area: ${currentGeo.GeoName}`);
                
                // Get the actual URL after the delay
                const actualMapUrl = CONFIG.MAP_URL();
                if (actualMapUrl === null || (actualMapUrl && typeof actualMapUrl === 'object')) {
                    // Something went wrong, skip this iteration
                    continue;
                }
                     const data = await runScraper(browser, actualMapUrl);
            
            // Check if we should retry due to server error
            if (data.shouldRetry) {
                console.log("Retrying current geo area due to server error...");
                continue; // Retry the same geo area without advancing
            }
            
            // Check if we should stop based on API response conditions
            if (data.shouldStop) {
                console.log("Stopping pagination due to API response conditions.");
                // Save data even if we're stopping (might have some results)
                if (data.Results && data.Results.length > 0) {
                    // Run detail scraping for this page's listings before saving data
                    await runDetailScrapingForPage(data.Results, browser, data);
                    // Remove the shouldStop flag before saving
                    const { shouldStop, ...dataToSave } = data;
                    await saveData(dataToSave, CONFIG.OUTPUT_FILE);
                    // Save final state
                    await saveScraperState();
                }
                break;
            }
            
            // Run detail scraping for the current page's listings before saving data
            await runDetailScrapingForPage(data.Results, browser, data);
            
            await saveData(data, CONFIG.OUTPUT_FILE);
            
            // Save scraper state after successful scrape
            await saveScraperState();
                
                isFirstPage = false;
                continue;
            }
            
            const mapUrl = mapUrlResult;
            
            // If CONFIG.MAP_URL is not a function, run once and break
            if (typeof CONFIG.MAP_URL !== 'function' && !isFirstPage) {
                console.log("CONFIG.MAP_URL is not a function. Running single page scrape.");
                break;
            }
            
            console.log(`\n--- Processing ${isFirstPage ? 'first' : 'next'} page ---`);
            const data = await runScraper(browser, mapUrl);
            
            // Check if we should retry due to server error
            if (data.shouldRetry) {
                console.log("Retrying current page due to server error...");
                continue; // Retry the same page without advancing
            }
            
            // Check if we should stop based on API response conditions
            if (data.shouldStop) {
                console.log("Stopping pagination due to API response conditions.");
                // Save data even if we're stopping (might have some results)
                if (data.Results && data.Results.length > 0) {
                    // Run detail scraping for this page's listings before saving data
                    await runDetailScrapingForPage(data.Results, browser, data);
                    // Remove the shouldStop flag before saving
                    const { shouldStop, ...dataToSave } = data;
                    await saveData(dataToSave, CONFIG.OUTPUT_FILE);
                    // Save final state
                    await saveScraperState();
                }
                break;
            }
            
            // Run detail scraping for the current page's listings (data saved per property inside)
            await runDetailScrapingForPage(data.Results, browser, data);
            
            // Final save after all details are processed (this is now redundant but kept for safety)
            await saveData(data, CONFIG.OUTPUT_FILE);

            // Save scraper state after successful scrape
            await saveScraperState();
            
            isFirstPage = false;
            
            // If CONFIG.MAP_URL is not a function, break after first iteration
            if (typeof CONFIG.MAP_URL !== 'function') {
                break;
            }

            // Add a randomized delay between pages to be respectful to the server
            const sleepMs = CONFIG.getSleepBetweenPages();
            console.log(`Waiting ${sleepMs / 1000} seconds before next page...`);
            await new Promise(resolve => setTimeout(resolve, sleepMs));
        }

        // Reset state file on successful completion for next cycle
        if (CONFIG.RESUME) {
            await resetScraperState();
        }
        
        console.log("Listings scraping completed successfully.");

    } catch (error) {
        console.error("Listings scraping error:", error.message || error);
    } finally {
        if (browser) {
            console.log("Closing listings browser.");
            await browser.close();
        }
    }
    
    console.log("\n=== INTEGRATED SCRAPING COMPLETED ===");
    console.log("Listings and details have been processed and saved together per page.");
    
    console.log("\n=== SCRIPT COMPLETED ===");
    console.log("Listings and details have been processed and saved together per page.");
}

// Execute
main().catch(err => {
     console.error("Unhandled Main Error:", err);
     process.exit(1); // Exit with error code
});