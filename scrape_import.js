#!/usr/bin/env node
// ============================================================
// scrape_import.js
//
// Integrated scraper + importer:
//   1. Scrapes listing pages from realtor.ca (via Puppeteer)
//   2. Scrapes detail pages per property (images, yearBuilt, etc.)
//   3. Streams images directly to Strapi (no local disk writes)
//   4. Creates/updates: cities, agents, amenities, properties
//   5. Approval flow:
//        - New property → created with approvalStatus = 'In Review'
//        - After images + data are fully imported → set to 'Approved'
//
// NOTE: This script does NOT save any JSON files or local images.
//       Resume state is still saved to ./data/scraper_import.json
//
// Usage:
//   node scrape_import.js
//
// Environment variables (see .env.example):
//   API_BASE_URL   - Strapi API base URL (default: http://localhost:1337)
//   API_TOKEN      - Strapi full-access API token (required)
//   SKIP_AMENITIES - Set to 'true' to skip amenity creation (default: false)
//   GOOGLE_MAPS_API_KEY - Optional, for geocoding missing coordinates
// ============================================================

// === 1. Dependencies ===
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fsPromises = require('node:fs/promises');
const fsSync = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const axios = require('axios');
const FormData = require('form-data');
const geoNames = require('./config/geoNames');

puppeteer.use(StealthPlugin());

// === 2. Load .env file ===
function loadEnvFile() {
    const envPath = path.join(__dirname, '.env');
    if (fsSync.existsSync(envPath)) {
        try {
            const envContent = fsSync.readFileSync(envPath, 'utf8');
            for (const line of envContent.split('\n')) {
                const trimmed = line.trim();
                if (!trimmed || trimmed.startsWith('#')) continue;
                const eqIdx = trimmed.indexOf('=');
                if (eqIdx <= 0) continue;
                const key = trimmed.substring(0, eqIdx).trim();
                let value = trimmed.substring(eqIdx + 1).trim().replace(/^["']|["']$/g, '').replace(/\s+$/, '');
                if (!process.env[key]) process.env[key] = value;
            }
        } catch (err) {
            console.warn(`Warning: could not read .env file: ${err.message}`);
        }
    }
}
loadEnvFile();

// === 3. Loop state for nested scraper loops ===
const transactionTypeIds = [2, 5]; // 2 = For Sale, 5 = Sold
let currentTransactionIndex = 0;
let currentGeoIndex = 0;
let currentPage = 0;

// === 4. Error retry tracking ===
let consecutiveErrorCount = 0;
const MAX_CONSECUTIVE_ERRORS = 3;

// === 5. In-memory caches (import side) ===
let provincesCache = null;
const citiesCache = new Map();    // key: "cityname_province" → cityDocumentId
const geocodeCache = new Map();   // key: address string → { lat, lng }
const strapiPropertyCache = new Map(); // key: "mls_X" or "id_X" → { documentId, imageCount }

// === 6. Central Configuration ===
const CONFIG = {
    // --- Scraper ---
    MAP_URL: () => {
        const baseUrl = 'https://www.realtor.ca/map#';
        const maxPages = 10;
        currentPage += 1;

        if (currentPage > maxPages) {
            currentPage = 1;
            currentGeoIndex += 1;
            if (currentGeoIndex >= geoNames.length) {
                currentGeoIndex = 0;
                currentTransactionIndex += 1;
                if (currentTransactionIndex >= transactionTypeIds.length) {
                    console.log('Completed all combinations. Stopping.');
                    return null;
                }
            }
            return { shouldDelayForGeo: true };
        }

        const currentTransactionId = transactionTypeIds[currentTransactionIndex];
        const currentGeo = geoNames[currentGeoIndex];
        console.log(`Generating URL for Transaction: ${currentTransactionId}, Geo: ${currentGeo.GeoName}, Page: ${currentPage}`);

        const params = {
            LatitudeMax: currentGeo.LatitudeMax,
            LongitudeMax: currentGeo.LongitudeMax,
            LatitudeMin: currentGeo.LatitudeMin,
            LongitudeMin: currentGeo.LongitudeMin,
            view: 'list',
            ...(currentPage > 1 ? { CurrentPage: currentPage.toString() } : {}),
            Sort: '6-D',
            PGeoIds: currentGeo.PGeoIds,
            GeoName: currentGeo.GeoName,
            PropertyTypeGroupID: '1',
            TransactionTypeId: currentTransactionId.toString(),
            PropertySearchTypeId: '0',
            Currency: 'CAD',
        };
        return `${baseUrl}${new URLSearchParams(params).toString()}`;
    },
    REALTOR_API_URL: 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post',
    USER_AGENT: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    TIMEOUT_MS: 60000,
    MAX_RETRY_ATTEMPTS: 2,
    RETRY_DELAY_MS: 5000,
    getSleepBetweenPages: () => {
        const min = 60000, max = 80000;
        return Math.floor(Math.random() * (max - min + 1)) + min;
    },
    getSleepBetweenGeos: () => {
        const min = 1111000, max = 3333000;
        return Math.floor(Math.random() * (max - min + 1)) + min;
    },
    HEADLESS: true,
    EXECUTABLE_PATH: '/usr/bin/google-chrome-stable',
    BLOCKED_RESOURCE_TYPES: ['image', 'media', 'font', 'stylesheet', 'other'],
    BLOCKED_URL_PATTERNS: ['.css', 'google-analytics', 'googletagmanager', 'doubleclick', 'scorecardresearch', 'youtube', 'intergient'],
    RESUME: true,
    STATE_FILE: './data/scraper_import.json',

    // --- Importer ---
    STRAPI_API_BASE_URL: (() => {
        const base = process.env.API_BASE_URL || 'http://localhost:1337';
        return base.endsWith('/api') ? base : base + '/api';
    })(),
    STRAPI_API_TOKEN: (process.env.API_TOKEN || '').trim(),
    CREATE_MISSING_CITIES: true,
    SKIP_AMENITIES: process.env.SKIP_AMENITIES === 'true',
    DEFAULT_CITY_IMAGE_URL: process.env.DEFAULT_CITY_IMAGE_URL || 'https://images.unsplash.com/photo-1480714378408-67cf0d13bc1b?w=800',
    IMPORT_DELAY_MS: 300, // delay between property imports to avoid overwhelming Strapi
    MAX_IMAGES_PER_PROPERTY: 50,
};

// === 7. State Management (resume support) ===
async function loadScraperState() {
    if (!CONFIG.RESUME) {
        return { currentTransactionIndex: 0, currentGeoIndex: 0, currentPage: 0 };
    }
    try {
        const content = await fsPromises.readFile(CONFIG.STATE_FILE, 'utf8');
        const state = JSON.parse(content);
        console.log(`Resuming from: Transaction ${state.currentTransactionIndex}, Geo ${state.currentGeoIndex}, Page ${state.currentPage}`);
        return state;
    } catch {
        console.log('No existing state file found. Starting from beginning.');
        return { currentTransactionIndex: 0, currentGeoIndex: 0, currentPage: 0 };
    }
}

async function saveScraperState() {
    try {
        await fsPromises.mkdir('./data', { recursive: true });
        const state = { currentTransactionIndex, currentGeoIndex, currentPage, lastUpdated: new Date().toISOString() };
        await fsPromises.writeFile(CONFIG.STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
        const geoName = geoNames[currentGeoIndex]?.GeoName || 'N/A';
        console.log(`State saved: Transaction ${currentTransactionIndex}, Geo ${currentGeoIndex} (${geoName}), Page ${currentPage}`);
    } catch (err) {
        console.warn(`Failed to save scraper state: ${err.message}`);
    }
}

async function resetScraperState() {
    try {
        await fsPromises.mkdir('./data', { recursive: true });
        const initial = { currentTransactionIndex: 0, currentGeoIndex: 0, currentPage: 0, lastUpdated: new Date().toISOString() };
        await fsPromises.writeFile(CONFIG.STATE_FILE, JSON.stringify(initial, null, 2), 'utf8');
        console.log('State file reset for next cycle.');
    } catch (err) {
        console.warn(`Failed to reset scraper state: ${err.message}`);
    }
}

// === 8. Error Handling (scraper) ===
async function handleServerError(errorType, details = '') {
    consecutiveErrorCount++;
    console.warn(`Server error (${errorType}): ${details}`);
    console.warn(`Consecutive error count: ${consecutiveErrorCount}/${MAX_CONSECUTIVE_ERRORS}`);
    if (consecutiveErrorCount >= MAX_CONSECUTIVE_ERRORS) {
        console.error(`Max consecutive errors reached. Exiting.`);
        process.exit(1);
    }
    const sleepMs = CONFIG.getSleepBetweenGeos();
    console.log(`Sleeping ${Math.round(sleepMs / 1000)}s due to server error...`);
    await new Promise(r => setTimeout(r, sleepMs));
}

function resetErrorCount() {
    if (consecutiveErrorCount > 0) {
        console.log('Successful response. Resetting error count.');
        consecutiveErrorCount = 0;
    }
}

function isServerError(statusCode) {
    return statusCode >= 400 && statusCode <= 599;
}

// === 9. Strapi API Helper ===
async function apiRequest(endpoint, method = 'GET', data = null, isFormData = false) {
    const token = CONFIG.STRAPI_API_TOKEN;
    if (!token) throw new Error('STRAPI_API_TOKEN is not set. Check .env file.');

    const config = {
        method,
        url: `${CONFIG.STRAPI_API_BASE_URL}${endpoint}`,
        headers: {
            Authorization: `Bearer ${token}`,
            ...(isFormData ? {} : { 'Content-Type': 'application/json' }),
        },
        timeout: 30000,
    };
    if (data) config.data = data;

    try {
        const response = await axios(config);
        return response.data;
    } catch (error) {
        if (error.code === 'ECONNREFUSED') {
            console.error(`❌ Cannot connect to Strapi at ${CONFIG.STRAPI_API_BASE_URL}. Is Strapi running?`);
            throw error;
        }
        if (error.response?.status === 401 || error.response?.status === 403) {
            console.error(`❌ Auth failed for ${endpoint} — check API_TOKEN (${method})`);
        } else if (error.response?.data?.error) {
            const e = error.response.data.error;
            console.error(`API Error [${method} ${endpoint}]: ${e.message}`);
            if (e.details) console.error('Details:', JSON.stringify(e.details, null, 2));
        } else {
            console.error(`API Error [${method} ${endpoint}]: ${error.message}`);
        }
        throw error;
    }
}

// === 10. Image Helpers (in-memory streaming — no disk writes) ===

/**
 * Downloads an image URL into a Buffer (no disk write).
 */
async function downloadImageToBuffer(url) {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(url);
        const client = parsedUrl.protocol === 'https:' ? https : http;
        const options = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            headers: {
                'User-Agent': CONFIG.USER_AGENT,
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Referer': 'https://www.realtor.ca/',
                'Host': parsedUrl.hostname,
            },
        };

        const req = client.get(options, (response) => {
            // Follow redirects
            if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
                return downloadImageToBuffer(response.headers.location).then(resolve).catch(reject);
            }
            if (response.statusCode !== 200) {
                return reject(new Error(`HTTP ${response.statusCode} for ${url}`));
            }
            const chunks = [];
            response.on('data', chunk => chunks.push(chunk));
            response.on('end', () => resolve(Buffer.concat(chunks)));
            response.on('error', reject);
        });

        req.on('error', reject);
        req.setTimeout(30000, () => {
            req.destroy();
            reject(new Error(`Timeout downloading ${url}`));
        });
    });
}

/**
 * Uploads an image Buffer to Strapi.
 * Returns the Strapi upload file object or null on failure.
 */
async function uploadImageFromBuffer(buffer, filename) {
    try {
        const form = new FormData();
        form.append('files', buffer, {
            filename,
            contentType: 'image/jpeg',
            knownLength: buffer.length,
        });

        const response = await axios({
            method: 'POST',
            url: `${CONFIG.STRAPI_API_BASE_URL}/upload`,
            headers: {
                Authorization: `Bearer ${CONFIG.STRAPI_API_TOKEN}`,
                ...form.getHeaders(),
            },
            data: form,
            timeout: 60000,
        });
        return response.data[0]; // Strapi upload returns array
    } catch (error) {
        console.error(`Failed to upload image ${filename}: ${error.message}`);
        return null;
    }
}

/**
 * Downloads image URLs (from a scraped detail page) and uploads them to Strapi.
 * Returns array of Strapi upload file objects.
 * No data is written to disk.
 */
async function uploadPropertyImagesFromUrls(propertyId, imageList) {
    if (!propertyId || !imageList || imageList.length === 0) return [];
    const uploaded = [];
    const limited = imageList.slice(0, CONFIG.MAX_IMAGES_PER_PROPERTY);

    for (let i = 0; i < limited.length; i++) {
        const image = limited[i];
        try {
            // Derive filename based on type
            const urlPath = new URL(image.url).pathname;
            const ext = path.extname(urlPath) || '.jpg';
            let filename;
            if (image.type === 'hero') {
                filename = `prop_${propertyId}_hero${ext}`;
            } else if (image.type === 'grid') {
                filename = `prop_${propertyId}_grid_${String(image.index ?? i).padStart(2, '0')}${ext}`;
            } else if (image.type === 'gallery') {
                filename = `prop_${propertyId}_gallery_${String(image.index ?? i).padStart(2, '0')}${ext}`;
            } else {
                filename = `prop_${propertyId}_img_${String(i).padStart(2, '0')}${ext}`;
            }

            console.log(`   📸 Downloading image ${i + 1}/${limited.length}: ${image.type} → ${filename}`);
            const buffer = await downloadImageToBuffer(image.url);
            const strapiFile = await uploadImageFromBuffer(buffer, filename);

            if (strapiFile) {
                uploaded.push(strapiFile);
                console.log(`   ✅ Uploaded: ${filename} (id: ${strapiFile.id})`);
            } else {
                console.warn(`   ⚠️  Upload returned null for ${filename}`);
            }
        } catch (err) {
            console.warn(`   ⚠️  Failed to process image ${i + 1}: ${err.message}`);
        }
    }
    return uploaded;
}

// === 11. Province / City helpers ===

async function getProvinces() {
    if (provincesCache) return provincesCache;
    try {
        const res = await apiRequest('/provinces');
        provincesCache = res.data || [];
        return provincesCache;
    } catch (err) {
        console.error('Failed to fetch provinces:', err.message);
        return [];
    }
}

async function findProvinceByName(provinceName) {
    const provinces = await getProvinces();
    return provinces.find(p =>
        p.name.toLowerCase() === provinceName.toLowerCase() ||
        p.name.toLowerCase().includes(provinceName.toLowerCase()) ||
        provinceName.toLowerCase().includes(p.name.toLowerCase())
    );
}

function createSlug(name) {
    return name.toLowerCase().trim()
        .replace(/[^\w\s-]/g, '')
        .replace(/[\s_-]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

async function getOrCreateCity(cityName, provinceName = 'British Columbia') {
    if (!cityName || cityName === 'Unknown' || !cityName.trim()) {
        cityName = 'Vancouver';
    }
    const clean = cityName.trim();
    const cacheKey = `${clean.toLowerCase()}_${provinceName.toLowerCase()}`;
    if (citiesCache.has(cacheKey)) return citiesCache.get(cacheKey);

    try {
        const slug = createSlug(clean);

        // Check by slug
        const slugRes = await apiRequest(`/cities?filters[slug][$eq]=${encodeURIComponent(slug)}`);
        if (slugRes.data?.length > 0) {
            const id = slugRes.data[0].documentId || slugRes.data[0].id;
            citiesCache.set(cacheKey, id);
            return id;
        }

        // Check by name (case-insensitive)
        const nameRes = await apiRequest(`/cities?filters[name][$eqi]=${encodeURIComponent(clean)}`);
        if (nameRes.data?.length > 0) {
            const id = nameRes.data[0].documentId || nameRes.data[0].id;
            citiesCache.set(cacheKey, id);
            return id;
        }

        if (!CONFIG.CREATE_MISSING_CITIES) throw new Error(`City not found: ${clean}`);

        console.log(`🏙️  Creating new city: ${clean}...`);
        const province = await findProvinceByName(provinceName);
        if (!province) throw new Error(`Province not found: ${provinceName}`);

        // Download default city image in-memory and upload to Strapi
        const imgBuffer = await downloadImageToBuffer(CONFIG.DEFAULT_CITY_IMAGE_URL);
        const cityImg = await uploadImageFromBuffer(imgBuffer, `city_${slug}_default.jpg`);
        if (!cityImg) throw new Error(`Failed to upload city image for ${clean}`);

        const newCity = await apiRequest('/cities', 'POST', {
            data: {
                name: clean,
                slug,
                province: province.documentId || province.id,
                media: cityImg.id,
            },
        });
        const cityId = newCity.data.documentId || newCity.data.id;
        console.log(`✅ Created city: ${clean} (${cityId})`);
        citiesCache.set(cacheKey, cityId);
        return cityId;
    } catch (err) {
        console.error(`Failed to get/create city ${clean}: ${err.message}`);
        // Fallback to Vancouver
        try {
            const fb = await apiRequest(`/cities?filters[slug][$eq]=vancouver&pagination[limit]=1`);
            if (fb.data?.length > 0) {
                const id = fb.data[0].documentId || fb.data[0].id;
                citiesCache.set('vancouver_british columbia', id);
                return id;
            }
        } catch {}
        throw err;
    }
}

// === 12. Amenity helpers ===

function normalizeAmenityName(name) {
    if (!name) return '';
    return name.trim().toLowerCase().split(' ')
        .map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

async function getOrCreateAmenities(names) {
    if (!names?.length) return [];
    const normalized = [...new Set(names.filter(n => n?.trim()).map(normalizeAmenityName))];
    const ids = [];
    for (const name of normalized) {
        try {
            const res = await apiRequest(`/amenities?filters[name][$eqi]=${encodeURIComponent(name)}`);
            if (res.data?.length > 0) {
                ids.push(res.data[0].documentId || res.data[0].id);
            } else {
                const created = await apiRequest('/amenities', 'POST', { data: { name } });
                ids.push(created.data.documentId || created.data.id);
                console.log(`   🏖️  Created amenity: ${name}`);
            }
        } catch (err) {
            console.warn(`   ⚠️  Failed to get/create amenity ${name}: ${err.message}`);
        }
    }
    return ids;
}

// === 13. Agent helpers ===

function extractBio(individual) {
    if (!individual) return '';
    let bio = individual.Organization?.Designation || '';
    if (individual.Designation && individual.Designation !== bio) {
        bio = bio ? `${bio}. ${individual.Designation}` : individual.Designation;
    }
    if (individual.Biography) bio = bio ? `${bio}. ${individual.Biography}` : individual.Biography;
    return bio.substring(0, 2000);
}

function extractSocialMedia(individual) {
    const s = { facebook: '', instagram: '', xTwitter: '', linkedIn: '', website: '' };
    if (!individual) return s;
    const extract = (websites) => {
        for (const w of (websites || [])) {
            const url = w.Website || w.url || w;
            if (!url) continue;
            const l = url.toLowerCase();
            if (l.includes('facebook.com')) s.facebook = s.facebook || url;
            else if (l.includes('instagram.com')) s.instagram = s.instagram || url;
            else if (l.includes('twitter.com') || l.includes('x.com')) s.xTwitter = s.xTwitter || url;
            else if (l.includes('linkedin.com')) s.linkedIn = s.linkedIn || url;
            else s.website = s.website || url;
        }
    };
    extract(individual.Websites);
    extract(individual.Organization?.Websites);
    return s;
}

async function getOrCreateAgent(individual) {
    if (!individual?.Name) return null;
    try {
        const agentId = individual.IndividualID || individual.Name.toLowerCase().replace(/\s+/g, '_');
        const username = `agent_${agentId}`;
        const email = `agent_${agentId}@smartdreamhome.ca`;

        // Check for existing user by username + email
        let existing = null;
        try {
            const byUser = await apiRequest(`/users?filters[username][$eq]=${encodeURIComponent(username)}&populate=profilePicture`);
            if (byUser?.length > 0) existing = byUser[0];
        } catch {}
        if (!existing) {
            try {
                const byEmail = await apiRequest(`/users?filters[email][$eq]=${encodeURIComponent(email)}&populate=profilePicture`);
                if (byEmail?.length > 0) existing = byEmail[0];
            } catch {}
        }

        if (existing) {
            const userId = existing.documentId || existing.id;
            // Upload profile picture if missing
            if (!existing.profilePicture && (individual.PhotoHighRes || individual.Photo)) {
                try {
                    const imgBuf = await downloadImageToBuffer(individual.PhotoHighRes || individual.Photo);
                    const pic = await uploadImageFromBuffer(imgBuf, `agent_${agentId}_profile.jpg`);
                    if (pic) await apiRequest(`/me/${userId}`, 'PUT', { profilePicture: pic.id });
                } catch {}
            }
            console.log(`✅ Agent exists: ${individual.Name} (${userId})`);
            return userId;
        }

        // Build phone info
        const phones = individual.Phones || [];
        const cellphone = phones[0] ? `${phones[0].AreaCode || ''}-${phones[0].PhoneNumber || ''}`.replace(/^-|-$/, '') : '';
        const landline = phones[1] ? `${phones[1].AreaCode || ''}-${phones[1].PhoneNumber || ''}`.replace(/^-|-$/, '')
            : (individual.Organization?.Phones?.[0]
                ? `${individual.Organization.Phones[0].AreaCode || ''}-${individual.Organization.Phones[0].PhoneNumber || ''}`.replace(/^-|-$/, '')
                : '');
        const nameParts = individual.Name.split(' ');
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';

        // Upload profile picture if available
        let profilePictureId = null;
        if (individual.PhotoHighRes || individual.Photo) {
            try {
                const imgBuf = await downloadImageToBuffer(individual.PhotoHighRes || individual.Photo);
                const pic = await uploadImageFromBuffer(imgBuf, `agent_${agentId}_profile.jpg`);
                if (pic) profilePictureId = pic.id;
            } catch (err) {
                console.warn(`   ⚠️  Could not upload profile pic for ${individual.Name}: ${err.message}`);
            }
        }

        // Register user
        const reg = await apiRequest('/auth/local/register', 'POST', { username, email, password: 'TempPassword123!' });
        if (!reg?.user) {
            console.warn(`Registration returned no user for ${individual.Name}`);
            return null;
        }

        const userId = reg.user.documentId || reg.user.id;

        // Build component data
        const businessInfo = {
            name: individual.Organization?.Name || '',
            bio: extractBio(individual),
            cellphone,
            landline,
            address: individual.Organization?.Address?.AddressText?.split('|')[0] || '',
            cities: [],
        };
        const socialMedia = extractSocialMedia(individual);

        const updatePayload = { accountType: 'Real Estate Agent' };
        if (firstName) updatePayload.firstName = firstName;
        if (lastName) updatePayload.lastName = lastName;
        if (cellphone || landline) updatePayload.phone = cellphone || landline;
        if (profilePictureId) updatePayload.profilePicture = profilePictureId;
        if (businessInfo.name || businessInfo.bio || businessInfo.cellphone || businessInfo.landline || businessInfo.address) {
            updatePayload.businessInfo = businessInfo;
        }
        if (Object.values(socialMedia).some(v => v?.trim())) {
            updatePayload.socialMedia = socialMedia;
        }

        const updated = await apiRequest(`/me/${userId}`, 'PUT', updatePayload);
        const finalId = updated?.data?.user?.documentId || updated?.data?.user?.id || userId;
        console.log(`✅ Created agent: ${individual.Name} (${finalId})`);
        return finalId;
    } catch (err) {
        console.warn(`Failed to get/create agent ${individual.Name}: ${err.message}`);
        return null;
    }
}

// === 14. Property Data Extraction  (from import.cjs) ===

function extractAddress(property) {
    if (property.Property?.Address?.AddressText) {
        return property.Property.Address.AddressText.split('|')[0].trim();
    }
    if (property.RelativeDetailsURL) {
        return property.RelativeDetailsURL.split('/').pop().replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }
    return 'Address Not Available';
}

function extractCityName(property) {
    if (property.Property?.Address?.AddressText) {
        const parts = property.Property.Address.AddressText.split('|');
        if (parts.length > 1) return parts[1].trim().split(',')[0].trim();
    }
    return 'Unknown';
}

function extractProvinceName(property) {
    return property.ProvinceName || 'British Columbia';
}

function extractPrice(property) {
    if (property.Property?.PriceUnformattedValue) return parseFloat(property.Property.PriceUnformattedValue);
    if (property.Property?.Price) {
        const str = property.Property.Price.replace(/[$,]/g, '');
        return parseFloat(str) || 1;
    }
    return 1;
}

function extractBedrooms(property) {
    if (property.Building?.Bedrooms) return parseFloat(property.Building.Bedrooms);
    const m = (property.PublicRemarks || '').match(/(\d+)\s*bed/i);
    return m ? parseFloat(m[1]) : 1;
}

function extractBathrooms(property) {
    if (property.Building?.BathroomTotal) return parseFloat(property.Building.BathroomTotal);
    const m = (property.PublicRemarks || '').match(/(\d+(?:\.\d+)?)\s*bath/i);
    return m ? parseFloat(m[1]) : 1;
}

function extractSize(property) {
    if (property.Building?.SizeInterior) {
        return parseFloat(property.Building.SizeInterior.replace(/[^\d.]/g, '')) || 1;
    }
    if (property.Building?.FloorAreaMeasurements?.length > 0) {
        const a = property.Building.FloorAreaMeasurements[0].Area || property.Building.FloorAreaMeasurements[0].AreaUnformatted;
        if (a) return parseFloat(a.replace(/[^\d.]/g, '')) || 1;
    }
    return 1;
}

function extractGarages(property) {
    if (property.Property?.Parking) {
        const garage = property.Property.Parking.find(p => p.Name?.toLowerCase().includes('garage'));
        if (garage) return garage.Spaces ? parseInt(garage.Spaces) : 1;
    }
    if (property.Property?.ParkingType?.toLowerCase().includes('garage')) return 1;
    return 0;
}

function extractYearBuilt(property) {
    const fromDetails = property.extractedDetails?.yearBuilt || property.extractedDetails?.propertyDetails?.Built_in;
    if (fromDetails) {
        const y = parseInt(fromDetails);
        if (y >= 1888 && y <= 3333) return y;
    }
    return 1900;
}

function extractLatitude(property) {
    const candidates = [
        property.Property?.Address?.Latitude,
        property.Latitude,
        property.lat,
        property.extractedDetails?.latitude,
        property.extractedDetails?.coordinates?.lat,
    ];
    for (const c of candidates) {
        if (c == null) continue;
        const v = parseFloat(c);
        if (!isNaN(v) && isFinite(v) && v >= -90 && v <= 90) return v;
    }
    return null;
}

function extractLongitude(property) {
    const candidates = [
        property.Property?.Address?.Longitude,
        property.Longitude,
        property.lng || property.lon,
        property.extractedDetails?.longitude,
        property.extractedDetails?.coordinates?.lng,
    ];
    for (const c of candidates) {
        if (c == null) continue;
        const v = parseFloat(c);
        if (!isNaN(v) && isFinite(v) && v >= -180 && v <= 180) return v;
    }
    return null;
}

function mapPropertyType(property) {
    const raw = (property.Building?.Type || '').toLowerCase().trim();
    const ownership = (property.Property?.OwnershipType || '').toLowerCase();

    if (raw.includes('/') && (raw.includes('duplex') || raw.includes('triplex') || raw.includes('fourplex'))) {
        const beds = extractBedrooms(property);
        if (beds >= 8) return 'Fourplex';
        if (beds >= 6) return 'Triplex';
        const size = extractSize(property);
        if (size > 4000) return 'Fourplex';
        if (size > 3000) return 'Triplex';
        return 'Duplex';
    }

    switch (raw) {
        case 'apartment': return 'Condo';
        case 'house': return 'Detached';
        case 'duplex': return 'Duplex';
        case 'triplex': return 'Triplex';
        case 'fourplex': case 'four-plex': case '4-plex': return 'Fourplex';
        case 'row / townhouse': case 'townhouse': case 'town house': return 'Townhouse';
        case 'semi-detached': case 'semi detached': case 'semi': return 'Semi-Detached';
        case 'mobile home': case 'mobile': return 'Mobile Home';
        case 'floating home': case 'floating': return 'Floating Home';
        case 'cottage': return 'Cottage';
        default:
            if (ownership === 'freehold') return 'Detached';
            if (ownership === 'strata') return 'Condo';
            return 'Detached';
    }
}

function mapListingStatus(property) {
    switch (property.StatusId) {
        case '1': return 'For Sale';
        case '2': return 'Sold';
        case '3': return 'Delisted';
        default: return 'Draft';
    }
}

function extractDescription(property) {
    return (property.PublicRemarks || '').substring(0, 999);
}

function extractAmenities(property) {
    const set = new Set();
    const add = (str) => str?.split(',').map(s => s.trim()).filter(Boolean).forEach(s => set.add(s));
    add(property.Building?.Ammenities);
    add(property.Property?.AmmenitiesNearBy);
    const remarks = (property.PublicRemarks || '').toLowerCase();
    ['pool', 'gym', 'fitness', 'parking', 'balcony', 'fireplace', 'air conditioning', 'laundry', 'storage', 'playground', 'garden']
        .forEach(a => { if (remarks.includes(a)) set.add(a.charAt(0).toUpperCase() + a.slice(1)); });
    return [...set];
}

function extractSummary(property) {
    const address = extractAddress(property);
    const price = extractPrice(property);
    const beds = extractBedrooms(property);
    const baths = extractBathrooms(property);
    const size = extractSize(property);
    const type = mapPropertyType(property);
    let s = `${type} at ${address}. ${beds} bed${beds !== 1 ? 's' : ''}, ${baths} bath${baths !== 1 ? 's' : ''}, ${size} sqft. $${price.toLocaleString()}.`;
    if (property.Building?.Ammenities) s += ` Amenities: ${property.Building.Ammenities}.`;
    if (property.Property?.AmmenitiesNearBy) s += ` Nearby: ${property.Property.AmmenitiesNearBy}.`;
    return s.substring(0, 3333);
}

// === 15. Geocoding ===

async function geocodeAddress(address) {
    if (geocodeCache.has(address)) return geocodeCache.get(address);
    const apiKey = process.env.GOOGLE_MAPS_API_KEY || process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY;
    if (!apiKey) return null;
    try {
        const res = await axios.get(`https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${apiKey}`, { timeout: 5000 });
        if (res.data?.status === 'OK' && res.data.results?.length > 0) {
            const loc = res.data.results[0].geometry.location;
            const result = { lat: loc.lat, lng: loc.lng };
            geocodeCache.set(address, result);
            await new Promise(r => setTimeout(r, 100));
            return result;
        }
    } catch {}
    return null;
}

// === 16. Property Mapping (import.cjs logic) ===

async function mapPropertyData(property) {
    const cityName = extractCityName(property);
    const cityId = await getOrCreateCity(cityName, extractProvinceName(property));

    // Get/create agent
    let userId = null;
    if (property.Individual?.length > 0) {
        userId = await getOrCreateAgent(property.Individual[0]);
        if (userId) {
            console.log(`   👤 Agent assigned: ${property.Individual[0].Name}`);
        }
    }

    // Amenities
    let amenityIds = [];
    if (!CONFIG.SKIP_AMENITIES) {
        amenityIds = await getOrCreateAmenities(extractAmenities(property));
    }

    // Coordinates
    let lat = extractLatitude(property);
    let lng = extractLongitude(property);
    if ((!lat || !lng) && property.Property?.Address?.AddressText) {
        const addr = extractAddress(property);
        if (addr && addr !== 'Address Not Available') {
            const full = property.Property.Address.AddressText.includes('|')
                ? property.Property.Address.AddressText.split('|').map(s => s.trim()).join(', ')
                : addr;
            const geo = await geocodeAddress(full);
            if (geo) {
                if (!lat) lat = geo.lat;
                if (!lng) lng = geo.lng;
            }
        }
    }

    const mapped = {
        title: extractAddress(property),
        price: extractPrice(property),
        beds: extractBedrooms(property),
        baths: extractBathrooms(property),
        size: extractSize(property),
        garages: extractGarages(property),
        yearBuilt: extractYearBuilt(property),
        description: extractDescription(property),
        summary: extractSummary(property),
        listingStatus: mapListingStatus(property),
        propertyType: mapPropertyType(property),
        taxRate: 0,
        address: extractAddress(property),
        postalCode: property.PostalCode || '',
        city: cityId,
        amenities: amenityIds,
    };

    if (userId) mapped.user = { set: [userId] };
    if (lat != null && !isNaN(lat)) mapped.latitude = lat;
    if (lng != null && !isNaN(lng)) mapped.longitude = lng;
    if (property.MlsNumber) mapped.mlsNumber = String(property.MlsNumber);

    return mapped;
}

// === 17. Strapi Property Existence Check ===

async function checkExistingProperty(mlsNumber, address = null, propertyId = null) {
    // Check local cache first
    const cacheKey = mlsNumber ? `mls_${mlsNumber}` : `id_${propertyId}`;
    if (strapiPropertyCache.has(cacheKey)) {
        const cached = strapiPropertyCache.get(cacheKey);
        if (cached.documentId) {
            // Fetch fresh data (we need populate=media for image count)
            try {
                const res = await apiRequest(`/properties/${cached.documentId}?populate=media`);
                return res.data || null;
            } catch {}
        }
        return null;
    }

    try {
        if (mlsNumber) {
            const r = await apiRequest(`/properties?filters[mlsNumber][$eq]=${encodeURIComponent(mlsNumber)}&populate=media`);
            if (r.data?.length > 0) {
                const prop = r.data[0];
                strapiPropertyCache.set(cacheKey, { documentId: prop.documentId || prop.id, imageCount: (prop.media || []).length });
                return prop;
            }
        }
        if (address) {
            const r = await apiRequest(`/properties?filters[address][$eq]=${encodeURIComponent(address)}&populate=media`);
            if (r.data?.length > 0) {
                const prop = r.data[0];
                if (mlsNumber) strapiPropertyCache.set(cacheKey, { documentId: prop.documentId || prop.id, imageCount: (prop.media || []).length });
                return prop;
            }
        }
        // Not found
        strapiPropertyCache.set(cacheKey, { documentId: null, imageCount: 0 });
        return null;
    } catch (err) {
        if (err.response?.status === 404) return null;
        console.warn(`Error checking existing property: ${err.message}`);
        return null;
    }
}

/**
 * Determines if detail scraping is needed for a property.
 * - Returns true if property doesn't exist in Strapi, or exists but has no images.
 * - Returns false if property exists with images (skip re-scraping).
 */
async function shouldScrapeDetails(mlsNumber, realtorPropertyId) {
    const existing = await checkExistingProperty(mlsNumber, null, realtorPropertyId);
    if (!existing) return { scrape: true, existing: null };
    const imageCount = (existing.media || []).length;
    if (imageCount === 0) return { scrape: true, existing };
    return { scrape: false, existing };
}

// === 18. Property Create / Update with Approval Flow ===

/**
 * Creates a new property in Strapi with the approval flow:
 *   1. Create with approvalStatus = 'In Review'
 *   2. After data + images are confirmed → update to 'Approved'
 *
 * NOTE: If the Strapi controller restricts approvalStatus writes for API tokens,
 * the property will remain 'In Review' and admin approval via the Strapi admin
 * panel will be required.
 */
async function createPropertyWithApproval(propertyData, uploadedImages) {
    const mappedData = await mapPropertyData(propertyData);

    // Attach uploaded images
    if (uploadedImages.length > 0) {
        mappedData.media = uploadedImages.map(img => img.id);
    }

    // Step 1: Create property (approvalStatus defaults to 'In Review' per schema)
    let createdDoc = null;
    try {
        const res = await apiRequest('/properties', 'POST', { data: { ...mappedData, status: 'published' } });
        createdDoc = res.data;
        console.log(`   ➕ Created property ${propertyData.MlsNumber || propertyData.Id} (documentId: ${createdDoc.documentId || createdDoc.id})`);
    } catch (err) {
        console.error(`   ❌ Failed to create property ${propertyData.MlsNumber || propertyData.Id}: ${err.message}`);
        return null;
    }

    // Step 2: Set approvalStatus to 'Approved' now that data + images are attached
    const docId = createdDoc.documentId || createdDoc.id;
    if (docId) {
        try {
            await apiRequest(`/properties/${docId}`, 'PUT', { data: { approvalStatus: 'Approved' } });
            console.log(`   ✅ Property ${propertyData.MlsNumber || propertyData.Id} set to Approved`);
        } catch (err) {
            console.warn(`   ⚠️  Could not set Approved for property ${docId}: ${err.message}`);
            console.warn(`      Property remains 'In Review'. Approve via Strapi admin panel.`);
        }
        // Cache result
        const cKey = propertyData.MlsNumber ? `mls_${propertyData.MlsNumber}` : `id_${propertyData.Id}`;
        strapiPropertyCache.set(cKey, { documentId: docId, imageCount: uploadedImages.length });
    }
    return createdDoc;
}

/**
 * Updates an existing property in Strapi and ensures approvalStatus = 'Approved'.
 */
async function updatePropertyWithApproval(propertyDocId, propertyData, uploadedImages, existingProperty) {
    const mappedData = await mapPropertyData(propertyData);

    if (uploadedImages.length > 0) {
        mappedData.media = uploadedImages.map(img => img.id);
    } else {
        // Don't overwrite existing media if we didn't re-upload
        delete mappedData.media;
    }

    // Always set to Approved on update (it was already imported before)
    mappedData.approvalStatus = 'Approved';

    // Remove fields that cause issues on update
    delete mappedData.provinceId;

    if (!mappedData.title?.trim()) {
        console.warn(`   ⚠️  Property ${propertyData.MlsNumber || propertyData.Id} has empty title — skipping update`);
        return null;
    }

    try {
        const res = await apiRequest(`/properties/${propertyDocId}`, 'PUT', { data: mappedData });
        console.log(`   ✅ Updated property ${propertyData.MlsNumber || propertyData.Id}`);
        // Update cache
        const cKey = propertyData.MlsNumber ? `mls_${propertyData.MlsNumber}` : `id_${propertyData.Id}`;
        strapiPropertyCache.set(cKey, { documentId: propertyDocId, imageCount: (res.data?.media || []).length });
        return res.data;
    } catch (err) {
        console.error(`   ❌ Failed to update property ${propertyData.MlsNumber || propertyData.Id}: ${err.message}`);
        if (err.response?.data?.error) {
            console.error('   API Error:', JSON.stringify(err.response.data.error, null, 2));
        }
        return null;
    }
}

// === 19. Core Scraper Logic (from scrape.js) ===

async function runScraper(browser, mapUrl) {
    let page;
    let lastError = null;

    for (let attempt = 1; attempt <= CONFIG.MAX_RETRY_ATTEMPTS + 1; attempt++) {
        console.log(`--- Scraping Attempt #${attempt}/${CONFIG.MAX_RETRY_ATTEMPTS + 1} ---`);
        try {
            if (page && !page.isClosed()) await page.close();
            page = await browser.newPage();

            page.on('pageerror', err => console.log(`[PAGE JS ERROR #${attempt}]: ${err}`));
            page.on('error', err => console.log(`[PAGE CRASH #${attempt}]: ${err}`));

            await page.setUserAgent(CONFIG.USER_AGENT);
            await page.setViewport({ width: 1280, height: 800 });
            await page.setRequestInterception(true);

            page.on('request', (request) => {
                const url = request.url();
                const type = request.resourceType();
                if (CONFIG.BLOCKED_RESOURCE_TYPES.includes(type) || CONFIG.BLOCKED_URL_PATTERNS.some(p => url.includes(p))) {
                    request.abort();
                } else {
                    request.continue();
                }
            });

            const apiResponsePromise = page.waitForResponse(
                res => res.url() === CONFIG.REALTOR_API_URL && res.request().method() === 'POST',
                { timeout: CONFIG.TIMEOUT_MS }
            );

            await page.goto(mapUrl, { waitUntil: 'networkidle0', timeout: CONFIG.TIMEOUT_MS });
            try { await page.evaluate(() => window.scrollBy(0, 100)); } catch {}

            const response = await apiResponsePromise;
            console.log(`API Response Status: ${response.status()}`);

            if (!response.ok()) {
                const text = await response.text();
                const msg = `API HTTP ${response.status()} ${response.statusText()}. Body: ${text.substring(0, 200)}`;
                if (isServerError(response.status())) {
                    if (page && !page.isClosed()) await page.close();
                    await handleServerError(`API ${response.status()}`, msg);
                    return { shouldRetry: true };
                }
                throw new Error(msg);
            }

            const data = await response.json();
            resetErrorCount();

            if (data.Paging?.RecordsPerPage === 0 || data.ErrorCode?.Id === 400) {
                if (page && !page.isClosed()) await page.close();
                return { ...data, shouldStop: true };
            }

            if (page && !page.isClosed()) await page.close();
            return data;
        } catch (err) {
            lastError = err;
            console.error(`Attempt #${attempt} failed: ${err.message}`);
            if (attempt > CONFIG.MAX_RETRY_ATTEMPTS) {
                if (page && !page.isClosed()) await page.close();
                throw lastError;
            }
            await new Promise(r => setTimeout(r, CONFIG.RETRY_DELAY_MS));
        }
    }

    if (page && !page.isClosed()) await page.close();
    throw lastError || new Error('Scraping failed after all attempts.');
}

// === 20. Property Detail Scraping (from scrape.js) ===

async function scrapePropertyDetail(browser, relativeUrl) {
    const fullUrl = `https://www.realtor.ca${relativeUrl}`;
    console.log(`   🔍 Scraping detail: ${fullUrl}`);

    const page = await browser.newPage();
    await page.setUserAgent(CONFIG.USER_AGENT);
    await page.setViewport({ width: 1280, height: 800 });
    await page.setRequestInterception(true);

    page.on('request', (request) => {
        const url = request.url();
        const type = request.resourceType();
        // Allow property images from realtor.ca CDN
        if (type === 'image' && url.includes('realtor.ca')) {
            request.continue();
            return;
        }
        if (CONFIG.BLOCKED_RESOURCE_TYPES.includes(type) || CONFIG.BLOCKED_URL_PATTERNS.some(p => url.includes(p))) {
            request.abort();
        } else {
            request.continue();
        }
    });

    try {
        const response = await page.goto(fullUrl, { waitUntil: 'networkidle0', timeout: CONFIG.TIMEOUT_MS });

        if (response && isServerError(response.status())) {
            await page.close();
            throw new Error(`HTTP ${response.status()} on detail page`);
        }

        const propertyDetails = await page.evaluate(() => {
            const extractText = sel => document.querySelector(sel)?.textContent?.trim() || null;
            const extractMultiple = sel => Array.from(document.querySelectorAll(sel)).map(el => el.textContent.trim()).filter(Boolean);

            const propertyId = window.location.pathname.match(/\/(\d+)\//)?.[1]
                || document.querySelector('[data-property-id]')?.getAttribute('data-property-id');

            let dataLayerInfo = {};
            if (typeof window.dataLayer !== 'undefined') {
                const layer = window.dataLayer.find(l => l.property);
                if (layer?.property) dataLayerInfo = layer.property;
            }

            const details = {
                Id: propertyId,
                extractedDetails: {
                    yearBuilt: extractText('.yearBuiltValue') || extractText('[data-year-built]'),
                    neighbourhood: dataLayerInfo.neighbourhood,
                    buildingStyle: dataLayerInfo.buildingStyle,
                    propertyDetails: {},
                    features: [],
                    images: [],
                    scrapedAt: new Date().toISOString(),
                    sourceUrl: window.location.href,
                },
            };

            // Property detail tables
            ['.propertyDetailsTable tr', '.property-details-table tr', '.listingDetailsTable tr',
                '.propertyDetailsSectionContentSubCon', '.propertyDetailsValueSubSectionCon']
                .forEach(sel => {
                    document.querySelectorAll(sel).forEach(row => {
                        let label, value;
                        if (sel.includes('SubCon')) {
                            label = row.querySelector('.propertyDetailsSectionContentLabel,.propertyDetailsValueSubSectionHeader');
                            value = row.querySelector('.propertyDetailsSectionContentValue,.propertyDetailsValueSubSectionValue');
                        } else {
                            label = row.querySelector('td:first-child,th:first-child,.label');
                            value = row.querySelector('td:last-child,td:nth-child(2),.value');
                        }
                        if (label?.textContent && value?.textContent) {
                            const k = label.textContent.trim().replace(':', '').replace(/\s+/g, '_');
                            const v = value.textContent.trim();
                            if (k && v && k !== v) details.extractedDetails.propertyDetails[k] = v;
                        }
                    });
                });

            // Features
            ['.featureList li', '.propertyFeatures li', '.amenities li', '.features li', '.listingFeatures li']
                .forEach(sel => { details.extractedDetails.features.push(...extractMultiple(sel)); });
            details.extractedDetails.features = [...new Set(details.extractedDetails.features)];

            // Images
            const images = [];
            const seen = new Set();
            const addImg = (src, type, index) => {
                if (src && src.includes('realtor.ca') && !seen.has(src)) {
                    seen.add(src);
                    images.push({ url: src, type, index });
                }
            };

            // Hero
            const hero = document.querySelector('img#heroImage');
            if (hero) addImg(hero.src || hero.getAttribute('data-src') || hero.getAttribute('data-original'), 'hero', 0);

            // Grid
            document.querySelectorAll('img.topGridViewListingImage').forEach((img, i) =>
                addImg(img.src || img.getAttribute('data-src') || img.getAttribute('data-original'), 'grid', i));

            // Gallery
            document.querySelectorAll('img.gridViewListingImage').forEach((img, i) =>
                addImg(img.src || img.getAttribute('data-src') || img.getAttribute('data-original'), 'gallery', i));

            // Fallback selectors
            ['.propertyPhoto img', '.listing-photo img', '.listingPhoto img', '.photoGallery img']
                .forEach(sel => document.querySelectorAll(sel).forEach((img, i) =>
                    addImg(img.src || img.getAttribute('data-src') || img.getAttribute('data-original'), 'other', i)));

            details.extractedDetails.images = images;
            return details;
        });

        await page.close();
        return propertyDetails;
    } catch (err) {
        console.error(`   ❌ Error scraping detail ${fullUrl}: ${err.message}`);
        await page.close();
        return null;
    }
}

// === 21. Detail Scraping + Import (combined per-page handler) ===

async function runDetailScrapingAndImportForPage(pageResults, browser) {
    if (!pageResults?.length) {
        console.log('No listings to process');
        return;
    }

    const itemsWithDetails = pageResults.filter(item => item.RelativeDetailsURL);
    console.log(`\n📋 Processing ${itemsWithDetails.length} properties for detail scraping + import`);

    let pageImported = 0, pageUpdated = 0, pageSkipped = 0, pageErrors = 0;

    for (let i = 0; i < itemsWithDetails.length; i++) {
        const item = itemsWithDetails[i];
        const realtorId = item.RelativeDetailsURL?.match(/\/(\d+)\//)?.[1];
        const mlsNumber = item.MlsNumber;
        const identifier = mlsNumber || realtorId || `item-${i}`;

        console.log(`\n[${i + 1}/${itemsWithDetails.length}] Property: ${identifier}`);

        try {
            // Quick validation
            const address = extractAddress(item);
            if (!address || address === 'Address Not Available') {
                console.log(`   ⚠️  Skipping — missing address`);
                pageSkipped++;
                continue;
            }
            const price = extractPrice(item);
            if (!price || price <= 0) {
                console.log(`   ⚠️  Skipping — invalid price: ${price}`);
                pageSkipped++;
                continue;
            }

            // Check whether we need to scrape detailed page
            const { scrape, existing } = await shouldScrapeDetails(mlsNumber, realtorId);

            let detailData = null;
            let uploadedImages = [];

            if (scrape) {
                // Scrape detail page for images + extra data
                console.log(`   🔎 Detail scraping needed (${existing ? 'no images' : 'new property'})`);
                try {
                    detailData = await scrapePropertyDetail(browser, item.RelativeDetailsURL);
                } catch (err) {
                    if (err.message.includes('HTTP 4') || err.message.includes('HTTP 5')) {
                        console.warn(`   ⚠️  HTTP error on detail page: ${err.message}`);
                        await handleServerError('Detail HTTP error', err.message);
                        i--; continue; // Retry same property
                    }
                    console.warn(`   ⚠️  Detail scrape failed: ${err.message}`);
                    detailData = null;
                }

                // Merge detail data into item
                if (detailData?.extractedDetails) {
                    item.extractedDetails = detailData.extractedDetails;
                }

                // Upload images directly to Strapi (in-memory, no disk writes)
                const imageList = detailData?.extractedDetails?.images || [];
                if (imageList.length > 0) {
                    console.log(`   🖼️  Uploading ${imageList.length} images to Strapi...`);
                    uploadedImages = await uploadPropertyImagesFromUrls(realtorId || identifier, imageList);
                    console.log(`   ✅ Uploaded ${uploadedImages.length}/${imageList.length} images`);
                } else {
                    console.log(`   ℹ️  No images found on detail page — creating without images`);
                }
            } else {
                console.log(`   ⚡ Skipping detail scrape — property already has images in Strapi`);
            }

            // Import to Strapi
            if (existing) {
                const docId = existing.documentId || existing.id;
                const result = await updatePropertyWithApproval(docId, item, uploadedImages, existing);
                if (result) pageUpdated++;
                else pageErrors++;
            } else {
                const result = await createPropertyWithApproval(item, uploadedImages);
                if (result) pageImported++;
                else pageErrors++;
            }

            // Reset consecutive error count on success
            resetErrorCount();

        } catch (err) {
            console.error(`   ❌ Error processing property ${identifier}: ${err.message}`);
            pageErrors++;
        }

        // Delay between properties to avoid overwhelming Strapi
        await new Promise(r => setTimeout(r, CONFIG.IMPORT_DELAY_MS));

        // Delay between detail page requests (same as scrape.js behaviour)
        if (scrape && i < itemsWithDetails.length - 1) {
            const sleepMs = CONFIG.getSleepBetweenPages();
            console.log(`   ⏳ Waiting ${(sleepMs / 1000).toFixed(0)}s before next detail page...`);
            await new Promise(r => setTimeout(r, sleepMs));
        }
    }

    console.log(`\n📊 Page summary: +${pageImported} created, ~${pageUpdated} updated, ⏭${pageSkipped} skipped, ❌${pageErrors} errors`);
}

// === 22. Main Orchestration ===

async function main() {
    console.log('🚀 scrape_import.js — Integrated scraper + importer starting...');

    // Validate API token before starting
    if (!CONFIG.STRAPI_API_TOKEN) {
        console.error('❌ API_TOKEN is not set!');
        console.error('   Set it in a .env file: API_TOKEN=your-strapi-full-access-token');
        console.error('   See .env.example for all available options.');
        process.exit(1);
    }

    console.log(`🌐 Strapi API: ${CONFIG.STRAPI_API_BASE_URL}`);
    console.log(`🔑 Token: ${CONFIG.STRAPI_API_TOKEN.substring(0, 20)}... (length: ${CONFIG.STRAPI_API_TOKEN.length})`);

    // Test Strapi connection
    try {
        await apiRequest('/properties?pagination[limit]=1');
        console.log('✅ Connected to Strapi API successfully\n');
    } catch (err) {
        if (err.code === 'ECONNREFUSED') {
            console.error(`❌ Cannot connect to Strapi at ${CONFIG.STRAPI_API_BASE_URL}`);
            console.error('   Make sure Strapi is running: cd api && npm run develop');
            process.exit(1);
        }
        console.warn(`⚠️  API connection test failed: ${err.message} — continuing anyway`);
    }

    // Load resume state
    const savedState = await loadScraperState();
    currentTransactionIndex = savedState.currentTransactionIndex;
    currentGeoIndex = savedState.currentGeoIndex;
    currentPage = savedState.currentPage;

    let browser;
    try {
        console.log(`🌐 Launching browser (headless: ${CONFIG.HEADLESS})...`);
        browser = await puppeteer.launch({
            headless: CONFIG.HEADLESS,
            executablePath: CONFIG.EXECUTABLE_PATH,
            args: ['--no-sandbox', '--disable-setuid-sandbox'],
            ignoreHTTPSErrors: true,
        });

        let isFirstPage = true;

        while (true) {
            const mapUrlResult = CONFIG.MAP_URL();

            if (mapUrlResult === null) {
                console.log('All geo/transaction combinations completed. Done.');
                break;
            }

            // Handle geo area switch (delay required)
            if (mapUrlResult?.shouldDelayForGeo) {
                const geoSleepMs = CONFIG.getSleepBetweenGeos();
                const geo = geoNames[currentGeoIndex];
                console.log(`\n=== Switching to new Geo: ${geo?.GeoName} — waiting ${Math.round(geoSleepMs / 1000)}s ===`);
                await new Promise(r => setTimeout(r, geoSleepMs));
                const actualUrl = CONFIG.MAP_URL();
                if (actualUrl === null || actualUrl?.shouldDelayForGeo) continue;

                const data = await runScraper(browser, actualUrl);
                if (data.shouldRetry) { continue; }
                if (data.shouldStop) {
                    if (data.Results?.length > 0) {
                        await runDetailScrapingAndImportForPage(data.Results, browser);
                        await saveScraperState();
                    }
                    break;
                }
                await runDetailScrapingAndImportForPage(data.Results, browser);
                await saveScraperState();
                isFirstPage = false;
                continue;
            }

            console.log(`\n--- Processing ${isFirstPage ? 'first' : 'next'} page ---`);
            const data = await runScraper(browser, mapUrlResult);

            if (data.shouldRetry) {
                console.log('Retrying page due to server error...');
                continue;
            }

            if (data.shouldStop) {
                console.log('Stopping — API indicates no more records.');
                if (data.Results?.length > 0) {
                    await runDetailScrapingAndImportForPage(data.Results, browser);
                    await saveScraperState();
                }
                break;
            }

            await runDetailScrapingAndImportForPage(data.Results, browser);
            await saveScraperState();

            isFirstPage = false;

            // Delay between pages
            const sleepMs = CONFIG.getSleepBetweenPages();
            console.log(`\n⏳ Waiting ${(sleepMs / 1000).toFixed(0)}s before next page...`);
            await new Promise(r => setTimeout(r, sleepMs));
        }

        if (CONFIG.RESUME) await resetScraperState();
        console.log('\n✅ All scraping + importing completed successfully.');

    } catch (err) {
        console.error('Fatal error in main:', err.message || err);
    } finally {
        if (browser) {
            console.log('Closing browser...');
            await browser.close();
        }
    }

    console.log('\n=== scrape_import.js COMPLETED ===');
}

main().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
});
