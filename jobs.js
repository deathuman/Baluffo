// Game Dev Jobs listing with fetching from Google Sheets
let allJobs = [];

// Curated fallback jobs (minimal - we have 30k+ from sheet)
const realJobs = [
  {
    id: 1,
    title: "Senior Gameplay Programmer",
    company: "Ubisoft San Francisco",
    country: "US",
    workType: "Onsite",
    profession: "gameplay",
    description: "We're looking for experienced gameplay programmers to develop engaging game mechanics using Unreal Engine 5. Join our AAA game development team."
  },
  {
    id: 2,
    title: "Graphics Programmer",
    company: "NVIDIA",
    country: "US",
    workType: "Hybrid",
    profession: "graphics",
    description: "Optimize rendering pipelines and implement cutting-edge graphics techniques for next-generation games. Work with NVIDIA's graphics technology."
  },
  {
    id: 3,
    title: "Game Designer",
    company: "Supercell",
    country: "FI",
    workType: "Remote",
    profession: "designer",
    description: "Design engaging gameplay experiences and systems for multiplayer games. Work with a team that develops games played by millions."
  },
  {
    id: 4,
    title: "3D Character Artist",
    company: "Artstation Learning",
    country: "US",
    workType: "Remote",
    profession: "artist",
    description: "Create high-quality 3D models and characters for AAA game projects. Use industry-standard tools like Maya and Substance."
  },
  {
    id: 5,
    title: "Engine Programmer",
    company: "Epic Games",
    country: "US",
    workType: "Onsite",
    profession: "engine",
    description: "Develop and maintain Unreal Engine systems. Deep C++ experience required. Work on systems used by millions worldwide."
  },
  {
    id: 6,
    title: "AI/Behavior Programmer",
    company: "Rockstar Games",
    country: "US",
    workType: "Onsite",
    profession: "ai",
    description: "Create sophisticated AI systems and behaviors for NPCs and enemies in large-scale open worlds."
  },
  {
    id: 7,
    title: "Tools Programmer",
    company: "Unity Technologies",
    country: "CA",
    workType: "Remote",
    profession: "tools",
    description: "Build and maintain internal developer tools and pipeline infrastructure. Improve game development workflows for thousands of developers."
  },
  {
    id: 8,
    title: "Animation Programmer",
    company: "Bungie Studio",
    country: "US",
    workType: "Hybrid",
    profession: "animator",
    description: "Develop animation systems and rigging tools. Work with animators to bring characters and creatures to life."
  },
  {
    id: 9,
    title: "Junior Gameplay Programmer",
    company: "Deck Nine Games",
    country: "GB",
    workType: "Remote",
    profession: "gameplay",
    description: "Start your game development career with an experienced indie studio. Work on narrative-driven games."
  },
  {
    id: 10,
    title: "Graphics Engineer",
    company: "Activision Blizzard",
    country: "US",
    workType: "Onsite",
    profession: "graphics",
    description: "Optimize graphics for cross-platform game delivery. Work on engines powering world-famous game franchises."
  },
  {
    id: 11,
    title: "Level Designer",
    company: "Remedy Entertainment",
    country: "FI",
    workType: "Hybrid",
    profession: "designer",
    description: "Design and balance game systems for action-oriented AAA titles. Shape player experiences and progression."
  },
  {
    id: 12,
    title: "Environment Artist",
    company: "FromSoftware",
    country: "JP",
    workType: "Onsite",
    profession: "artist",
    description: "Model stunning environments and props for immersive game worlds. Experience with next-gen console development."
  },
  {
    id: 13,
    title: "Physics Programmer",
    company: "Insomniac Games",
    country: "US",
    workType: "Onsite",
    profession: "engine",
    description: "Develop physics systems for dynamic game environments. Work on acclaimed PlayStation game franchises."
  },
  {
    id: 14,
    title: "Senior Game Designer",
    company: "Valve",
    country: "US",
    workType: "Onsite",
    profession: "designer",
    description: "Lead game design on groundbreaking projects. Influence the direction of iconic game franchises."
  },
  {
    id: 15,
    title: "VFX Artist",
    company: "Splash Damage",
    country: "GB",
    workType: "Remote",
    profession: "artist",
    description: "Create stunning visual effects for competitive multiplayer games. Use Unreal Engine and modern tools."
  },
  {
    id: 16,
    title: "Network Programmer",
    company: "Electronic Arts",
    country: "US",
    workType: "Hybrid",
    profession: "gameplay",
    description: "Develop robust networking systems for multiplayer games. Handle millions of concurrent players."
  },
  {
    id: 17,
    title: "Gameplay Programmer",
    company: "Mighty Games",
    country: "CA",
    workType: "Remote",
    profession: "gameplay",
    description: "Join a fast-growing indie studio creating innovative mobile games. Flexible work environment."
  },
  {
    id: 18,
    title: "3D Artist",
    company: "Weta Digital",
    country: "AU",
    workType: "Onsite",
    profession: "artist",
    description: "Work on cutting-edge visual effects and 3D assets for blockbuster productions and games."
  },
  {
    id: 19,
    title: "Game Audio Designer",
    company: "Bandcamp Games",
    country: "US",
    workType: "Remote",
    profession: "designer",
    description: "Create immersive audio experiences and sound design for indie and indie-inspired games."
  },
  {
    id: 20,
    title: "Engine Optimization Specialist",
    company: "GrainWorks",
    country: "SG",
    workType: "Remote",
    profession: "engine",
    description: "Optimize game engines for performance across multiple platforms. Work with cutting-edge technology."
  },
  {
    id: 21,
    title: "UI/UX Designer",
    company: "Motion Twin",
    country: "FR",
    workType: "Remote",
    profession: "designer",
    description: "Design engaging user interfaces and experiences for indie game titles played worldwide."
  },
  {
    id: 22,
    title: "Character Rigger",
    company: "Axis Animation",
    country: "GB",
    workType: "Onsite",
    profession: "animator",
    description: "Create character rigs and skeletal systems for animation teams. Work on AAA game projects."
  }
];

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
        
        if (!title || !company) continue;
        
        // Work type: map location type to our format
        let workType = "Onsite";
        if (locationType.toLowerCase().includes('remote')) workType = "Remote";
        else if (locationType.toLowerCase().includes('hybrid')) workType = "Hybrid";
        
        jobs.push({
          id: 1000 + i,
          title: escapeHtml(title),
          company: escapeHtml(company),
          country: city && city !== company ? `${city}, ${country}` : country,
          workType: workType,
          profession: mapProfession(title),
          description: `${title} at ${company}`
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
  
  if (lower.includes('gameplay') || lower.includes('game mechanics')) return 'gameplay';
  if (lower.includes('graphics') || lower.includes('rendering') || lower.includes('shader')) return 'graphics';
  if (lower.includes('engine') || lower.includes('architecture') || lower.includes('systems')) return 'engine';
  if (lower.includes('ai') || lower.includes('artificial intelligence') || lower.includes('behavior')) return 'ai';
  if (lower.includes('tool') || lower.includes('pipeline') || lower.includes('editor')) return 'tools';
  if (lower.includes('designer') || lower.includes('level') || lower.includes('game design')) return 'designer';
  if (lower.includes('artist') || lower.includes('animation') || lower.includes('visual')) return 'artist';
  if (lower.includes('animator') || lower.includes('motion')) return 'animator';
  
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
let jobsList, backBtn, workTypeFilter, countryFilter, professionFilter, searchFilter;

// Wait for DOM to load, then initialize
document.addEventListener("DOMContentLoaded", function() {
  jobsList = document.getElementById("jobs-list");
  backBtn = document.getElementById("back-btn");
  workTypeFilter = document.getElementById("work-type-filter");
  countryFilter = document.getElementById("country-filter");
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
  if (professionFilter) professionFilter.addEventListener("change", filterAndDisplay);
  if (searchFilter) searchFilter.addEventListener("input", filterAndDisplay);
  
  // Load jobs (async)
  init().catch(err => console.error("Error initializing jobs:", err));
});

// Initialize
async function init() {
  if (jobsList) {
    jobsList.innerHTML = '<div class="loading">Loading game development jobs...</div>';
    
    // Try fetching from Google Sheets first
    const sheetJobs = await fetchFromGoogleSheets();
    
    if (sheetJobs && sheetJobs.length > 0) {
      allJobs = sheetJobs;
      jobsList.innerHTML = '';
    } else {
      allJobs = realJobs;
      jobsList.innerHTML = '';
    }
    
    updateFilterOptions();
    filterAndDisplay();
  }
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
  if (!workTypeFilter || !countryFilter || !professionFilter) return;

  const countries = new Set();
  const professions = new Set();

  allJobs.forEach(job => {
    if (job.country) countries.add(job.country);
    if (job.profession) professions.add(job.profession);
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


  professionFilter.innerHTML = '<option value="">All Roles</option>';
  Array.from(professions)
    .sort()
    .forEach(p => {
      const opt = document.createElement('option');
      opt.value = p;
      opt.textContent = capitalizeFirst(p) + (p === 'gameplay' ? ' Programmer' : (p === 'graphics' ? ' Programmer' : (p === 'engine' ? ' Programmer' : (p === 'ai' ? ' Programmer' : (p === 'tools' ? ' Programmer' : (p === 'designer' ? ' Designer' : (p === 'artist' ? ' Artist' : (p === 'animator' ? ' Animator' : ''))))))));
      professionFilter.appendChild(opt);
    });
}

// Filter and display jobs
function filterAndDisplay() {
  if (!jobsList) return;
  
  const workType = workTypeFilter ? workTypeFilter.value : "";
  const country = countryFilter ? countryFilter.value : "";
  const profession = professionFilter ? professionFilter.value : "";
  const searchTerm = searchFilter ? searchFilter.value.toLowerCase() : "";

  const filtered = allJobs.filter(job => {
    const matchesWorkType = !workType || job.workType === workType;
    const matchesCountry = !country || job.country === country;
    const matchesProfession = !profession || job.profession === profession;
    const matchesSearch =
      !searchTerm ||
      job.title.toLowerCase().includes(searchTerm) ||
      job.company.toLowerCase().includes(searchTerm);

    return matchesWorkType && matchesCountry && matchesProfession && matchesSearch;
  });

  displayJobs(filtered);
}

// Display jobs as compact rows
function displayJobs(jobs) {
  if (!jobsList) return;
  
  if (jobs.length === 0) {
    jobsList.innerHTML = '<div class="no-results">No jobs found matching your filters.</div>';
    return;
  }

  jobsList.innerHTML = `
    <div class="jobs-table-header">
      <div class="job-row-header">
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-location">Location</div>
        <div class="col-type">Type</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${jobs.map(job => `
        <div class="job-row">
          <div class="col-title">
            <div class="job-title-compact">${escapeHtml(job.title)}</div>
          </div>
          <div class="col-company">
            <span class="job-company-compact">${escapeHtml(job.company)}</span>
          </div>
          <div class="col-location">
            <span class="job-location">${escapeHtml(job.country)}</span>
          </div>
          <div class="col-type">
            <span class="job-tag ${job.workType.toLowerCase()}">${capitalizeFirst(job.workType)}</span>
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

// Utility functions
function capitalizeFirst(str) {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1);
}

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
