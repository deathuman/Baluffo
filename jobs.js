// Game Dev Jobs listing with fetching from Google Sheets
let allJobs = [];

// pagination state
let currentPage = 1;
const itemsPerPage = 20; // show 20 jobs per page

// read initial page from URL query string if present
function readPageFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const p = parseInt(params.get('page'), 10);
  if (!isNaN(p) && p > 0) {
    currentPage = p;
  }
}

// update URL without reloading
function updateUrlPage() {
  const params = new URLSearchParams(window.location.search);
  if (currentPage > 1) {
    params.set('page', currentPage);
  } else {
    params.delete('page');
  }
  const newUrl = window.location.pathname + '?' + params.toString();
  window.history.replaceState({}, '', newUrl);
}

// attach keyboard navigation once DOM is ready
function enableKeyboardNav() {
  document.addEventListener('keydown', e => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.isContentEditable) {
      // ignore when typing in filters
      return;
    }
    if (e.key === 'ArrowLeft' && currentPage > 1) {
      currentPage--;
      updateUrlPage();
      filterAndDisplay();
    }
    if (e.key === 'ArrowRight') {
      // we'll compute if there is a next page inside displayJobs itself
      // by simply incrementing and re-displaying
      currentPage++;
      updateUrlPage();
      filterAndDisplay();
    }
  });
}

// Fetch jobs from Google Sheets CSV export - optimized for large datasets
async function fetchFromGoogleSheets() {
  try {
    const sheetId = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE";
    const gid = "1560329579";
    const csvUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv&gid=${gid}`;
    
    let csv = null;
    
    // Approach 1: Try direct CSV export (most reliable for Google Sheets)
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 20000);
      
      const response = await fetch(csvUrl, {
        signal: controller.signal,
        mode: 'cors',
        credentials: 'omit'
      });
      clearTimeout(timeoutId);
      
      if (response.ok) {
        csv = await response.text();
      }
    } catch (err) {
      // Direct fetch failed, try proxy
    }
    
    // Approach 2: Try alternative CORS proxy if direct failed
    if (!csv) {
      try {
        const corsUrl = `https://cors-anywhere.herokuapp.com/${csvUrl}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 25000);
        
        const response = await fetch(corsUrl, {
          signal: controller.signal,
          credentials: 'omit'
        });
        clearTimeout(timeoutId);
        
        if (response.ok) {
          csv = await response.text();
        }
      } catch (err) {
        // CORS proxy also failed
      }
    }
    
    if (!csv || csv.length < 100) {
      return null;
    }
    
    const jobs = parseCSVLarge(csv);
    return jobs.length > 0 ? jobs : null;
  } catch (error) {
    console.error("Error fetching Google Sheets:", error.message);
    return null;
  }
}

// Parse CSV with optimization for large datasets and metadata handling
function parseCSVLarge(csv) {
  try {
    const startTime = performance.now();
    
    // Split by newline
    const lines = csv.split('\n');
    
    // Find the actual header row
    let headerLineIdx = 0;
    for (let i = 0; i < Math.min(30, lines.length); i++) {
      const line = lines[i].toLowerCase();
      if ((line.includes('title') || line.includes('job category')) && 
          (line.includes('city') || line.includes('company name'))) {
        headerLineIdx = i;
        break;
      }
    }
    
    if (headerLineIdx >= lines.length - 1) {
      return [];
    }
    
    // Parse header
    const headerLine = lines[headerLineIdx];
    const headers = parseCSVLine(headerLine).map(h => h.toLowerCase().trim());
    
    // Find column indices
    const companyIdx = findColumnIndex(headers, ['company name']);
    const titleIdx = findColumnIndex(headers, ['title']);
    const cityIdx = findColumnIndex(headers, ['city']);
    const countryIdx = findColumnIndex(headers, ['country']);
    const locationTypeIdx = findColumnIndex(headers, ['location type']);
    const jobLinkIdx = findColumnIndex(headers, ['job link']);
    
    if (titleIdx === -1 || companyIdx === -1) {
      return [];
    }
    
    // Parse data rows
    const jobs = [];
    const startRow = headerLineIdx + 1;
    const totalLines = lines.length;
    
    for (let i = startRow; i < totalLines; i++) {
      try {
        const line = lines[i].trim();
        if (line.length === 0) continue;
        
        const fields = parseCSVLine(lines[i]);
        
        const title = fields[titleIdx]?.trim() || "";
        const company = fields[companyIdx]?.trim() || "";
        const city = fields[cityIdx]?.trim() || "";
        const country = fields[countryIdx]?.trim() || "Unknown";
        const locationType = fields[locationTypeIdx]?.trim() || "On-site";
        const jobLink = jobLinkIdx !== -1 ? fields[jobLinkIdx]?.trim() : "";
        
        if (!title || !company) continue;
        
        // Work type: map location type to our format
        let workType = "Onsite";
        if (locationType.toLowerCase().includes('remote')) workType = "Remote";
        else if (locationType.toLowerCase().includes('hybrid')) workType = "Hybrid";
        
        // Store country separately and keep city for additional information
        jobs.push({
          id: 1000 + i,
          title: escapeHtml(title),
          company: escapeHtml(company),
          city: city,             // raw city text
          country: country,       // raw country text only
          workType: workType,
          profession: mapProfession(title),
          description: `${title} at ${company}`,
          jobLink: jobLink        // job listing URL
        });
        
        // Log progress every 10000 rows
        if (jobs.length % 10000 === 0) {
          console.log(`Loaded ${jobs.length} jobs...`);
        }
      } catch (err) {
        // Skip malformed rows silently
      }
    }
    
    const endTime = performance.now();
    console.log(`Successfully loaded ${jobs.length} jobs in ${((endTime - startTime) / 1000).toFixed(2)}s`);
    
    return jobs;
  } catch (err) {
    console.error("Error parsing CSV:", err.message);
    return [];
  }
}

// Helper: Parse a single CSV line handling quotes and escaped commas
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let insideQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      insideQuotes = !insideQuotes;
    } else if (char === ',' && !insideQuotes) {
      result.push(current.trim().replace(/^"|"$/g, ''));
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current.trim().replace(/^"|"$/g, ''));
  return result;
}

// Helper: Find column index by matching multiple possible names
function findColumnIndex(headers, possibleNames) {
  for (let i = 0; i < headers.length; i++) {
    for (const name of possibleNames) {
      if (headers[i].includes(name)) {
        return i;
      }
    }
  }
  return -1;
}

// Detect work type from text
function detectWorkType(text) {
  if (!text) return "Onsite";
  const lower = text.toLowerCase();
  if (lower.includes("remote")) return "Remote";
  if (lower.includes("hybrid") || lower.includes("mixed")) return "Hybrid";
  return "Onsite";
}

// Map job title to profession category
function mapProfession(title) {
  const lower = title.toLowerCase();
  
  // unique technical artist category
  if (lower.includes('technical artist')) return 'technical-artist';
  
  // other mappings
  if (lower.includes('gameplay') || lower.includes('game mechanics')) return 'gameplay';
  if (lower.includes('graphics') || lower.includes('rendering') || lower.includes('shader')) return 'graphics';
  if (lower.includes('engine') || lower.includes('architecture') || lower.includes('systems')) return 'engine';
  if (lower.includes('ai') || lower.includes('artificial intelligence') || lower.includes('behavior')) return 'ai';
  if (lower.includes('animator') || lower.includes('motion')) return 'animator';
  // technical alone should map to tools
  if (lower.includes('tool') || lower.includes('pipeline') || lower.includes('editor') || (lower.includes('technical') && !lower.includes('artist'))) return 'tools';
  if (lower.includes('designer') || lower.includes('level') || lower.includes('game design')) return 'designer';
  if (lower.includes('artist') || lower.includes('animation') || lower.includes('visual')) return 'artist';
  
  return 'other';
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
  if (!text) return "";
  const map = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;"
  };
  return text.replace(/[&<>"']/g, m => map[m]);
}

// Capitalize first letter
function capitalizeFirst(str) {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// DOM Elements
let jobsList, backBtn, workTypeFilter, countryFilter, cityFilter, professionFilter, searchFilter;

// Wait for DOM to load, then initialize
document.addEventListener("DOMContentLoaded", function() {
  jobsList = document.getElementById("jobs-list");
  backBtn = document.getElementById("back-btn");
  workTypeFilter = document.getElementById("work-type-filter");
  countryFilter = document.getElementById("country-filter");
  cityFilter = document.getElementById("city-filter");
  professionFilter = document.getElementById("profession-filter");
  searchFilter = document.getElementById("search-filter");
  
  // Set up back button
  if (backBtn) {
    backBtn.addEventListener("click", function() {
      window.location.href = "index.html";
    });
  }
  
  // Set up filter listeners
  if (workTypeFilter) workTypeFilter.addEventListener("change", filterAndDisplay);
  if (countryFilter) countryFilter.addEventListener("change", filterAndDisplay);
  if (cityFilter) cityFilter.addEventListener("change", filterAndDisplay);
  if (professionFilter) professionFilter.addEventListener("change", filterAndDisplay);
  if (searchFilter) searchFilter.addEventListener("input", filterAndDisplay);
  
  // Load jobs (async)
  init().catch(err => console.error("Error initializing jobs:", err));
});

// Initialize
async function init() {
  if (jobsList) {
    jobsList.innerHTML = '<div class="loading">Loading game development jobs...</div>';
    
    // Show progress bar
    const progressBar = document.getElementById('fetch-progress');
    if (progressBar) {
      progressBar.classList.remove('hidden');
    }
    
    // read initial page before fetching data
    readPageFromUrl();
    
    // Try fetching from Google Sheets first
    const sheetJobs = await fetchFromGoogleSheets();
    
    // Hide progress bar
    if (progressBar) {
      progressBar.classList.add('hidden');
    }
    
    if (sheetJobs && sheetJobs.length > 0) {
      allJobs = sheetJobs;
      jobsList.innerHTML = '';
      updateFilterOptions();
      filterAndDisplay();
      // enable arrow navigation after display
      enableKeyboardNav();
    } else {
      jobsList.innerHTML = '<div class="error">Unable to load job listings at this time. Please try again later.</div>';
    }
  }
}

// Validate country entry - filter out malformed data
function isValidCountry(country) {
  if (!country || typeof country !== 'string') return false;
  const trimmed = country.trim();
  if (!trimmed) return false;
  // Skip entries with commas or double commas (malformed)
  if (trimmed.includes(',')) return false;
  // Skip very short entries that are likely errors
  if (trimmed.length < 2) return false;
  return true;
}

// Return full country name for code
function fullCountryName(code) {
  const map = {
    US: "United States",
    CA: "Canada",
    GB: "United Kingdom",
    DE: "Germany",
    FI: "Finland",
    JP: "Japan",
    AU: "Australia",
    SG: "Singapore",
    FR: "France",
    Remote: "Remote"
  };
  return map[code] || code;
}

// Build dynamic filter option lists based on jobs
function updateFilterOptions() {
  if (!workTypeFilter || !countryFilter || !professionFilter || !cityFilter) return;

  const countries = new Set();
  const professions = new Set();
  const cities = new Set();

  allJobs.forEach(job => {
    // country now contains only country codes/names, not city prefixes
    if (isValidCountry(job.country)) countries.add(job.country);
    if (job.profession) professions.add(job.profession);
    if (job.city) cities.add(job.city);
  });

  // clear existing except default
  countryFilter.innerHTML = '<option value="">All Countries</option>';
  Array.from(countries)
    .sort()
    .forEach(c => {
      const opt = document.createElement('option');
      opt.value = c;
      opt.textContent = fullCountryName(c);
      countryFilter.appendChild(opt);
    });

  // populate city filter
  cityFilter.innerHTML = '<option value="">All Cities</option>';
  Array.from(cities)
    .sort()
    .forEach(city => {
      const opt = document.createElement('option');
      opt.value = city;
      opt.textContent = city;
      cityFilter.appendChild(opt);
    });



  professionFilter.innerHTML = '<option value="">All Roles</option>';
  Array.from(professions)
    .sort()
    .forEach(p => {
      const opt = document.createElement('option');
      opt.value = p;
      
      // explicit labels avoid duplicate suffixes
      const labels = {
        gameplay: 'Gameplay Programmer',
        graphics: 'Graphics Programmer',
        engine: 'Engine Programmer',
        ai: 'AI Programmer',
        tools: 'Tools Programmer',
        'technical-artist': 'Technical Artist',
        designer: 'Game Designer',
        artist: 'Artist',
        animator: 'Animator',
        other: 'Other'
      };
      opt.textContent = labels[p] || capitalizeFirst(p);
      professionFilter.appendChild(opt);
    });
}

// Filter and display jobs
function filterAndDisplay() {
  if (!jobsList) return;

  // whenever the filter changes we start on page 1
  currentPage = 1;
  updateUrlPage();

  const workType = workTypeFilter ? workTypeFilter.value : "";
  const country = countryFilter ? countryFilter.value : "";
  const city = cityFilter ? cityFilter.value : "";
  const profession = professionFilter ? professionFilter.value : "";
  const searchTerm = searchFilter ? searchFilter.value.toLowerCase() : "";

  const filtered = allJobs.filter(job => {
    const matchesWorkType = !workType || job.workType === workType;
    const matchesCountry = !country || job.country === country;
    const matchesCity = !city || job.city === city;
    const matchesProfession = !profession || job.profession === profession;
    const matchesSearch =
      !searchTerm ||
      job.title.toLowerCase().includes(searchTerm) ||
      job.company.toLowerCase().includes(searchTerm) ||
      job.city?.toLowerCase().includes(searchTerm);

    return matchesWorkType && matchesCountry && matchesCity && matchesProfession && matchesSearch;
  });

  displayJobs(filtered);
}

// Display jobs as compact rows
function displayJobs(jobs) {
  if (!jobsList) return;

  if (jobs.length === 0) {
    jobsList.innerHTML = '<div class="no-results">No jobs found matching your filters.</div>';
    document.getElementById('pagination').innerHTML = '';
    return;
  }

  const totalPages = Math.ceil(jobs.length / itemsPerPage);
  if (currentPage > totalPages) currentPage = totalPages;
  // we may have clamped page, push back to url
  updateUrlPage();

  const startIndex = (currentPage - 1) * itemsPerPage;
  const pageJobs = jobs.slice(startIndex, startIndex + itemsPerPage);

  // build table for just the slice
  jobsList.innerHTML = `
    <div class="jobs-table-header">
      <div class="job-row-header">
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-type">Type</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${pageJobs.map(job => `
        <div class="job-row" ${job.jobLink ? `onclick="window.open('${escapeHtml(job.jobLink)}', '_blank')" style="cursor: pointer;"` : ''}>
          <div class="col-title">
            <div class="job-title-compact">${escapeHtml(job.title)}</div>
          </div>
          <div class="col-company">
            <span class="job-company-compact">${escapeHtml(job.company)}</span>
          </div>
          <div class="col-city">
            <span class="job-location">${escapeHtml(job.city || '')}</span>
          </div>
          <div class="col-country">
            <span class="job-location">${escapeHtml(fullCountryName(job.country))}</span>
          </div>
          <div class="col-type">
            <span class="job-tag ${job.workType.toLowerCase()}">${capitalizeFirst(job.workType)}</span>
          </div>
        </div>
      `).join('')}
    </div>
  `;

  // render pagination buttons
  const pagContainer = document.getElementById('pagination');
  if (pagContainer) {
    let html = '';
    if (totalPages > 1) {
      if (currentPage > 1) {
        html += `<button class="page-btn" data-page="${currentPage - 1}">Prev</button>`;
      }

      // compute which page numbers to show (with ellipsis when needed)
      const visible = [];
      if (totalPages <= 9) {
        // show all pages
        for (let p = 1; p <= totalPages; p++) visible.push(p);
      } else {
        visible.push(1);
        let left = currentPage - 2;
        let right = currentPage + 2;
        if (left <= 2) {
          left = 2;
          right = 5;
        }
        if (right >= totalPages - 1) {
          right = totalPages - 1;
          left = totalPages - 4;
        }
        if (left > 2) visible.push('...');
        for (let p = left; p <= right; p++) visible.push(p);
        if (right < totalPages - 1) visible.push('...');
        visible.push(totalPages);
      }

      visible.forEach(item => {
        if (item === '...') {
          html += `<span class="page-ellipsis">…</span>`;
        } else {
          const p = item;
          html += `<button class="page-btn ${p === currentPage ? 'active' : ''}" data-page="${p}">${p}</button>`;
        }
      });

      if (currentPage < totalPages) {
        html += `<button class="page-btn" data-page="${currentPage + 1}">Next</button>`;
      }
    }
    pagContainer.innerHTML = html;

    // attach listeners
    pagContainer.querySelectorAll('.page-btn').forEach(btn => {
      btn.addEventListener('click', function () {
        const page = parseInt(this.dataset.page, 10);
        if (!isNaN(page) && page !== currentPage) {
          currentPage = page;
          displayJobs(jobs);
        }
      });
    });
  }
}


