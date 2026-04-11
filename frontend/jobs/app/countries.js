import { fullCountryName as fullCountryNameFromData } from "../../shared/data/index.js";
import { COUNTRY_ACCEPTANCE } from "../../shared/data/country-acceptance.js";
import {
  canonicalizeCountryName,
  fullCountryName as fullCountryNameFromDomainLayer,
  normalizeCountryToken
} from "../domain.js";
import {
  getAvailableRegionOptions as getAvailableRegionOptionsFromModule,
  getCountryFilterOptionLabel as getCountryFilterOptionLabelFromModule,
  matchesCountrySelection as matchesCountrySelectionFromModule,
  resolveCountryCode as resolveCountryCodeFromModule,
  resolveRegionSelection as resolveRegionSelectionFromModule,
  countryMatchesRegion as countryMatchesRegionFromModule
} from "./filters.js";

const COUNTRY_DISPLAY_NAMES = (typeof Intl !== "undefined" && typeof Intl.DisplayNames === "function")
  ? new Intl.DisplayNames(["en"], { type: "region" })
  : null;

const COUNTRY_NAME_BY_CODE = COUNTRY_ACCEPTANCE.countryNameByCode || {};

const COUNTRY_ALIAS_TO_CANONICAL = Object.fromEntries(COUNTRY_ACCEPTANCE.aliasToCanonical);

const COUNTRY_NAME_OPTIONS = {
  fullCountryNameFromData,
  countryNamesByCode: COUNTRY_NAME_BY_CODE,
  countryAliasToCanonical: COUNTRY_ALIAS_TO_CANONICAL,
  countryDisplayNames: COUNTRY_DISPLAY_NAMES
};

const REGION_DEFINITIONS = [
  {
    value: "region:europe",
    label: "Europe",
    countries: [
      "Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria",
      "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia", "Finland", "France", "Germany",
      "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein",
      "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands",
      "North Macedonia", "Norway", "Poland", "Portugal", "Romania", "San Marino", "Serbia",
      "Slovakia", "Slovenia", "Spain", "Sweden", "Switzerland", "Ukraine", "United Kingdom",
      "Vatican City"
    ]
  },
  {
    value: "region:north-america",
    label: "North America",
    countries: [
      "Antigua and Barbuda", "Bahamas", "Barbados", "Belize", "Canada", "Costa Rica", "Cuba",
      "Dominica", "Dominican Republic", "El Salvador", "Grenada", "Guatemala", "Haiti", "Honduras",
      "Jamaica", "Mexico", "Nicaragua", "Panama", "Saint Kitts and Nevis", "Saint Lucia",
      "Saint Vincent and the Grenadines", "Trinidad and Tobago", "United States"
    ]
  },
  {
    value: "region:south-america",
    label: "South America",
    countries: [
      "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador", "Guyana", "Paraguay",
      "Peru", "Suriname", "Uruguay", "Venezuela"
    ]
  },
  {
    value: "region:asia",
    label: "Asia",
    countries: [
      "Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Bhutan", "Brunei", "Cambodia",
      "China", "Georgia", "India", "Indonesia", "Iran", "Iraq", "Israel", "Japan", "Jordan",
      "Kazakhstan", "Kuwait", "Kyrgyzstan", "Laos", "Lebanon", "Malaysia", "Maldives", "Mongolia",
      "Myanmar", "Nepal", "North Korea", "Oman", "Pakistan", "Palestine", "Philippines", "Qatar",
      "Russia", "Saudi Arabia", "Singapore", "South Korea", "Sri Lanka", "Syria", "Taiwan",
      "Tajikistan", "Thailand", "Timor-Leste", "Turkey", "Turkmenistan", "United Arab Emirates",
      "Uzbekistan", "Vietnam", "Yemen"
    ]
  },
  {
    value: "region:africa",
    label: "Africa",
    countries: [
      "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde", "Cameroon",
      "Central African Republic", "Chad", "Comoros", "Congo", "Democratic Republic of the Congo",
      "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
      "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Kenya", "Lesotho", "Liberia", "Libya",
      "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia",
      "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone",
      "Somalia", "South Africa", "South Sudan", "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda",
      "Zambia", "Zimbabwe"
    ]
  },
  {
    value: "region:oceania",
    label: "Oceania",
    countries: [
      "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Zealand",
      "Palau", "Papua New Guinea", "Samoa", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu"
    ]
  },
  {
    value: "region:remote-worldwide",
    label: "Remote / Worldwide",
    countries: ["Remote", "Worldwide", "Global"]
  }
];

const COUNTRY_COMPATIBILITY_LABELS = ["Anywhere", "England", "EU & NA", "Global", "Remote", "Worldwide"];

const REMOTE_WORLDWIDE_TOKENS = new Set(
  ["remote", "worldwide", "global", "anywhere"].map(item => normalizeCountryToken(item))
);

const REGION_COUNTRY_TOKEN_LOOKUP = Object.fromEntries(
  REGION_DEFINITIONS.map(region => [
    region.value,
    new Set(region.countries.map(item => normalizeCountryToken(canonicalizeCountryName(item, COUNTRY_NAME_OPTIONS))).filter(Boolean))
  ])
);

function countryMatchesRegion(countryToken, regionValue) {
  return countryMatchesRegionFromModule(countryToken, regionValue, {
    regionCountryTokenLookup: REGION_COUNTRY_TOKEN_LOOKUP,
    remoteWorldwideTokens: REMOTE_WORLDWIDE_TOKENS
  });
}

export function getCountryFilterOptionLabel(value) {
  return getCountryFilterOptionLabelFromModule(value, {
    regionDefinitions: REGION_DEFINITIONS,
    fullCountryName: fullCountryNameFromDomainLayer,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}

export function fullCountryName(value) {
  return fullCountryNameFromDomainLayer(value, COUNTRY_NAME_OPTIONS);
}

export function getAvailableRegionOptions(countries) {
  return getAvailableRegionOptionsFromModule(countries, {
    canonicalizeCountryName,
    normalizeCountryToken,
    regionDefinitions: REGION_DEFINITIONS,
    regionCountryTokenLookup: REGION_COUNTRY_TOKEN_LOOKUP,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}

export function getSupportedCountryLabels() {
  return Array.from(
    new Set([
      ...Object.keys(COUNTRY_NAME_BY_CODE),
      ...Object.values(COUNTRY_NAME_BY_CODE),
      ...REGION_DEFINITIONS.flatMap(region => region.countries),
      ...COUNTRY_COMPATIBILITY_LABELS
    ])
  );
}

export function matchesCountrySelection(jobCountry, selections) {
  return matchesCountrySelectionFromModule(jobCountry, selections, {
    canonicalizeCountryName,
    normalizeCountryToken,
    countryNameOptions: COUNTRY_NAME_OPTIONS,
    regionCountryMatcher: countryMatchesRegion
  });
}

export function resolveCountryCode(countryCode, {
  availableCountries,
  availableCountryFilterValues
}) {
  return resolveCountryCodeFromModule(countryCode, {
    availableCountries,
    availableCountryFilterValues,
    resolveRegionValue: value => resolveRegionSelectionFromModule(value, {
      normalizeCountryToken,
      regionDefinitions: REGION_DEFINITIONS
    }),
    canonicalizeCountryName,
    normalizeCountryToken,
    countryNameOptions: COUNTRY_NAME_OPTIONS
  });
}
