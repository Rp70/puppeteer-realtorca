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
const mode = args[0] || 'list'; // Default to 'list' mode

if (!['list', 'detail'].includes(mode)) {
    console.error('Usage: node scrape.js [list|detail]');
    console.error('  list   - Scrape listings from the map URL (default)');
    console.error('  detail - Extract details from saved listings');
    process.exit(1);
}

// State tracking for nested loops
let currentTransactionIndex = 0;
let currentGeoIndex = 0;
let currentPage = 0;

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
    DETAIL_OUTPUT_FILE: './data/property_details.json',
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

async function saveScraperState(detailProgress = null) {
    try {
        // Ensure data directory exists
        await fs.mkdir('./data', { recursive: true });
        
        const state = {
            currentTransactionIndex,
            currentGeoIndex,
            currentPage,
            lastUpdated: new Date().toISOString()
        };
        
        // Add detail progress if provided
        if (detailProgress) {
            state.detailProgress = detailProgress;
        }
        
        await fs.writeFile(CONFIG.STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
        const currentGeoName = geoNames[currentGeoIndex] ? geoNames[currentGeoIndex].GeoName : 'N/A';
        console.log(`State saved: Transaction Index ${currentTransactionIndex}, Geo Index: ${currentGeoIndex} (${currentGeoName}), Page Index: ${currentPage}`);
        
        if (detailProgress) {
            console.log(`Detail progress: ${detailProgress.processedPropertyIds.length} properties processed`);
        }
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

async function loadDetailProgress() {
    try {
        const stateContent = await fs.readFile(CONFIG.STATE_FILE, 'utf8');
        const state = JSON.parse(stateContent);
        
        if (state.detailProgress && Array.isArray(state.detailProgress.processedPropertyIds)) {
            console.log(`Resuming detail scraping from ${state.detailProgress.processedPropertyIds.length} processed properties`);
            return state.detailProgress;
        }
    } catch (error) {
        console.log("No existing detail progress found. Starting from beginning.");
    }
    
    return {
        processedPropertyIds: [],
        lastProcessedIndex: -1,
        lastUpdated: new Date().toISOString()
    };
}

async function saveDetailProgress(processedPropertyIds, currentIndex) {
    const detailProgress = {
        processedPropertyIds: processedPropertyIds,
        lastProcessedIndex: currentIndex,
        lastUpdated: new Date().toISOString()
    };
    
    await saveScraperState(detailProgress);
}

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

// 6. Image Download Functions
async function downloadImage(url, filePath) {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(url);
        const client = parsedUrl.protocol === 'https:' ? https : http;
        
        const request = client.get(url, (response) => {
            if (response.statusCode === 200) {
                const fileStream = require('fs').createWriteStream(filePath);
                response.pipe(fileStream);
                
                fileStream.on('finish', () => {
                    fileStream.close();
                    resolve(filePath);
                });
                
                fileStream.on('error', (err) => {
                    require('fs').unlink(filePath, () => {}); // Delete partial file
                    reject(err);
                });
            } else if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
                // Handle redirect
                downloadImage(response.headers.location, filePath).then(resolve).catch(reject);
            } else {
                reject(new Error(`Failed to download image: ${response.statusCode} ${response.statusMessage}`));
            }
        });
        
        request.on('error', reject);
        request.setTimeout(30000, () => {
            request.destroy();
            reject(new Error('Download timeout'));
        });
    });
}

async function savePropertyImages(propertyId, images) {
    if (!propertyId || !images || images.length === 0) {
        return [];
    }
    
    const propertyDir = `./data/properties/${propertyId}`;
    const imagesDir = `${propertyDir}/images`;
    
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
            await downloadImage(image.url, filePath);
            
            savedImages.push({
                ...image,
                localPath: filePath,
                filename: filename
            });
            
            console.log(`Saved: ${filename}`);
            
        } catch (error) {
            console.warn(`Failed to download image ${image.url}: ${error.message}`);
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

    // Apply the same blocking rules as the main scraper for efficiency
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
    
    try {
        await page.goto(fullUrl, {
            waitUntil: 'networkidle0',
            timeout: CONFIG.TIMEOUT_MS
        });
        
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
        
        await page.close();
        return propertyDetails;
        
    } catch (error) {
        console.error(`Error scraping ${fullUrl}: ${error.message}`);
        await page.close();
        return null;
    }
}

async function runDetailScraper() {
    console.log("Starting detail scraper...");
    
    // Load detail progress
    const detailProgress = await loadDetailProgress();
    
    // Read the listings file
    let listingsData;
    try {
        const content = await fs.readFile(CONFIG.OUTPUT_FILE, 'utf8');
        listingsData = JSON.parse(content);
    } catch (error) {
        console.error(`Error reading listings file: ${error.message}`);
        return;
    }
    
    if (!listingsData.Results || !Array.isArray(listingsData.Results)) {
        console.error("No results found in listings file");
        return;
    }
    
    // Filter items with RelativeDetailsURL
    const itemsWithDetails = listingsData.Results.filter(item => item.RelativeDetailsURL);
    console.log(`Found ${itemsWithDetails.length} listings with detail URLs`);
    
    if (itemsWithDetails.length === 0) {
        console.log("No listings with detail URLs found");
        return;
    }
    
    // Filter out already processed items based on property ID
    const itemsToProcess = itemsWithDetails.filter(item => {
        const propertyId = item.RelativeDetailsURL?.match(/\/(\d+)\//)?.[1];
        return propertyId && !detailProgress.processedPropertyIds.includes(propertyId);
    });
    
    console.log(`Found ${itemsToProcess.length} new items to process (${detailProgress.processedPropertyIds.length} already processed)`);
    
    if (itemsToProcess.length === 0) {
        console.log("All items have already been processed");
        return;
    }
    
    // Launch browser
    console.log(`Launching browser for detail scraping (Headless: ${CONFIG.HEADLESS})...`);
    const browser = await puppeteer.launch({
        headless: CONFIG.HEADLESS,
        executablePath: CONFIG.EXECUTABLE_PATH,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        ignoreHTTPSErrors: true,
    });
    
    // Load existing processed properties
    let detailedProperties = [];
    try {
        const existingContent = await fs.readFile(CONFIG.DETAIL_OUTPUT_FILE, 'utf8');
        const existingData = JSON.parse(existingContent);
        if (existingData.properties && Array.isArray(existingData.properties)) {
            detailedProperties = existingData.properties;
        }
    } catch (error) {
        console.log("No existing detail output file found, starting fresh");
    }
    
    let processed = detailProgress.processedPropertyIds.length;
    
    try {
        for (let i = 0; i < itemsToProcess.length; i++) {
            const item = itemsToProcess[i];
            const propertyId = item.RelativeDetailsURL?.match(/\/(\d+)\//)?.[1];
            
            processed++;
            console.log(`Processing ${processed}/${itemsWithDetails.length}: ${item.RelativeDetailsURL} (Property ID: ${propertyId})`);
            
            const details = await scrapePropertyDetail(browser, item.RelativeDetailsURL);
            if (details) {
                // Download and save images to structured directory
                console.log(`Found ${details.extractedDetails.images.length} images for property ${details.Id}`);
                const savedImages = await savePropertyImages(details.Id, details.extractedDetails.images);
                
                // Update the details with saved image information
                details.extractedDetails.images = savedImages;
                details.extractedDetails.imagesSaved = savedImages.length;
                details.extractedDetails.imagesDirectory = `./data/properties/${details.Id}/images/`;
                
                // Save only the extracted real estate information
                detailedProperties.push(details);
                
                // Add to processed property IDs
                if (propertyId) {
                    detailProgress.processedPropertyIds.push(propertyId);
                }
                
                // Save progress after every scrape (both detail output and state)
                await fs.writeFile(
                    CONFIG.DETAIL_OUTPUT_FILE, 
                    JSON.stringify({
                        totalProcessed: processed,
                        totalItems: itemsWithDetails.length,
                        scrapedAt: new Date().toISOString(),
                        properties: detailedProperties
                    }, null, 2), 
                    'utf8'
                );
                
                // Save detail progress to state file
                await saveDetailProgress(detailProgress.processedPropertyIds, i);
                
                console.log(`Progress saved: ${processed}/${itemsWithDetails.length} processed (${savedImages.length} images saved)`);
            }
            
            // Add delay between requests
            const sleepMs = CONFIG.getSleepBetweenPages();
            console.log(`Waiting ${sleepMs / 1000} seconds before next detail page...`);
            await new Promise(resolve => setTimeout(resolve, sleepMs));
        }
        
        // Save final results
        await fs.writeFile(
            CONFIG.DETAIL_OUTPUT_FILE, 
            JSON.stringify({
                totalProcessed: processed,
                totalItems: itemsWithDetails.length,
                scrapedAt: new Date().toISOString(),
                properties: detailedProperties
            }, null, 2), 
            'utf8'
        );
        
        // Clear detail progress from state since we're done
        if (itemsToProcess.length > 0) {
            await saveDetailProgress([], -1);
        }
        
        console.log(`Detail scraping completed. Processed ${processed} total properties (${itemsToProcess.length} new).`);
        console.log(`Results saved to: ${CONFIG.DETAIL_OUTPUT_FILE}`);
        
    } catch (error) {
        console.error(`Detail scraping error: ${error.message}`);
    } finally {
        await browser.close();
    }
}

// 7. Main Orchestration Function
async function main() {
     console.log(`Script start in ${mode} mode.`);
     
     // Handle detail mode
     if (mode === 'detail') {
         await runDetailScraper();
         console.log("Detail scraping completed.");
         return;
     }
     
     // List mode (original functionality)
     let browser;
     
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
                
                // Check if we should stop based on API response conditions
                if (data.shouldStop) {
                    console.log("Stopping pagination due to API response conditions.");
                    // Save data even if we're stopping (might have some results)
                    if (data.Results && data.Results.length > 0) {
                        // Remove the shouldStop flag before saving
                        const { shouldStop, ...dataToSave } = data;
                        await saveData(dataToSave, CONFIG.OUTPUT_FILE);
                        // Save final state
                        await saveScraperState();
                    }
                    break;
                }
                
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
            
            // Check if we should stop based on API response conditions
            if (data.shouldStop) {
                console.log("Stopping pagination due to API response conditions.");
                // Save data even if we're stopping (might have some results)
                if (data.Results && data.Results.length > 0) {
                    // Remove the shouldStop flag before saving
                    const { shouldStop, ...dataToSave } = data;
                    await saveData(dataToSave, CONFIG.OUTPUT_FILE);
                    // Save final state
                    await saveScraperState();
                }
                break;
            }
            
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