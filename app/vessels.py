"""Plain-language vessel descriptions from AIS: flag state from the MMSI, type from the AIS code.

The flag comes from the Maritime Identification Digits (MID), the first three
digits of a ship's MMSI, allocated by the ITU. AIS dimensions are the antenna's
distance to bow, stern, port and starboard, so length and beam are approximate.
"""

# ITU Maritime Identification Digits -> country or territory.
_MID_RANGES = {
    "Albania": [201], "Andorra": [202], "Austria": [203], "Portugal (Azores)": [204], "Belgium": [205],
    "Belarus": [206], "Bulgaria": [207], "Vatican City": [208], "Cyprus": [209, 210, 212], "Germany": [211, 218],
    "Georgia": [213], "Moldova": [214], "Malta": [215, 229, 248, 249, 256], "Armenia": [216],
    "Denmark": [219, 220], "Spain": [224, 225], "France": [226, 227, 228], "Finland": [230],
    "Faroe Islands": [231], "United Kingdom": [232, 233, 234, 235], "Gibraltar": [236],
    "Greece": [237, 239, 240, 241], "Croatia": [238], "Morocco": [242], "Hungary": [243],
    "Netherlands": [244, 245, 246], "Italy": [247], "Ireland": [250], "Iceland": [251], "Liechtenstein": [252],
    "Luxembourg": [253], "Monaco": [254], "Portugal (Madeira)": [255], "Norway": [257, 258, 259],
    "Poland": [261], "Montenegro": [262], "Portugal": [263], "Romania": [264], "Sweden": [265, 266],
    "Slovakia": [267], "San Marino": [268], "Switzerland": [269], "Czechia": [270], "Turkey": [271],
    "Ukraine": [272], "Russia": [273], "North Macedonia": [274], "Latvia": [275], "Estonia": [276],
    "Lithuania": [277], "Slovenia": [278], "Serbia": [279],
    "Anguilla": [301], "Antigua and Barbuda": [304, 305], "Curaçao": [306], "Aruba": [307],
    "Bahamas": [308, 309, 311], "Bermuda": [310], "Belize": [312], "Barbados": [314], "Canada": [316],
    "Cayman Islands": [319], "Costa Rica": [321], "Cuba": [323], "Dominica": [325], "Dominican Republic": [327],
    "Guadeloupe": [329], "Grenada": [330], "Greenland": [331], "Guatemala": [332], "Honduras": [334],
    "Haiti": [336], "United States": [303, 338, 366, 367, 368, 369], "Jamaica": [339],
    "Saint Kitts and Nevis": [341], "Saint Lucia": [343], "Mexico": [345], "Martinique": [347],
    "Montserrat": [348], "Nicaragua": [350], "Panama": [351, 352, 353, 354, 355, 356, 357, 370, 371, 372, 373, 374],
    "Puerto Rico": [358], "El Salvador": [359], "Saint Pierre and Miquelon": [361], "Trinidad and Tobago": [362],
    "Turks and Caicos Islands": [364], "Saint Vincent and the Grenadines": [375, 376, 377],
    "British Virgin Islands": [378], "US Virgin Islands": [379],
    "Afghanistan": [401], "Saudi Arabia": [403], "Bangladesh": [405], "Bahrain": [408], "Bhutan": [410],
    "China": [412, 413, 414], "Taiwan": [416], "Sri Lanka": [417], "India": [419], "Iran": [422],
    "Azerbaijan": [423], "Iraq": [425], "Israel": [428], "Japan": [431, 432], "Turkmenistan": [434],
    "Kazakhstan": [436], "Uzbekistan": [437], "Jordan": [438], "South Korea": [440, 441], "Palestine": [443],
    "North Korea": [445], "Kuwait": [447], "Lebanon": [450], "Kyrgyzstan": [451], "Macao": [453],
    "Maldives": [455], "Mongolia": [457], "Nepal": [459], "Oman": [461], "Pakistan": [463], "Qatar": [466],
    "Syria": [468], "United Arab Emirates": [470, 471], "Tajikistan": [472], "Yemen": [473, 475],
    "Hong Kong": [477], "Bosnia and Herzegovina": [478],
    "Australia": [503], "Myanmar": [506], "Brunei": [508], "Micronesia": [510], "Palau": [511],
    "New Zealand": [512], "Cambodia": [514, 515], "Cook Islands": [518], "Fiji": [520], "Indonesia": [525],
    "Kiribati": [529], "Laos": [531], "Malaysia": [533], "Northern Mariana Islands": [536],
    "Marshall Islands": [538], "New Caledonia": [540], "Niue": [542], "Nauru": [544], "French Polynesia": [546],
    "Philippines": [548], "Timor-Leste": [550], "Papua New Guinea": [553], "Solomon Islands": [557],
    "American Samoa": [559], "Samoa": [561], "Singapore": [563, 564, 565, 566], "Thailand": [567],
    "Tonga": [570], "Tuvalu": [572], "Vietnam": [574], "Vanuatu": [576, 577], "Wallis and Futuna": [578],
    "South Africa": [601], "Angola": [603], "Algeria": [605], "Burundi": [609], "Benin": [610], "Botswana": [611],
    "Central African Republic": [612], "Cameroon": [613], "Congo": [615], "Comoros": [616, 620], "Cabo Verde": [617],
    "Côte d'Ivoire": [619], "Djibouti": [621], "Egypt": [622], "Ethiopia": [624], "Eritrea": [625], "Gabon": [626],
    "Ghana": [627], "Gambia": [629], "Guinea-Bissau": [630], "Equatorial Guinea": [631], "Guinea": [632],
    "Burkina Faso": [633], "Kenya": [634], "Liberia": [636, 637], "South Sudan": [638], "Libya": [642],
    "Lesotho": [644], "Mauritius": [645], "Madagascar": [647], "Mali": [649], "Mozambique": [650],
    "Mauritania": [654], "Malawi": [655], "Niger": [656], "Nigeria": [657], "Namibia": [659], "Réunion": [660],
    "Rwanda": [661], "Sudan": [662], "Senegal": [663], "Seychelles": [664], "Saint Helena": [665],
    "Somalia": [666], "Sierra Leone": [667], "São Tomé and Príncipe": [668], "Eswatini": [669], "Chad": [670],
    "Togo": [671], "Tunisia": [672], "Tanzania": [674, 677], "Uganda": [675], "DR Congo": [676], "Zambia": [678],
    "Zimbabwe": [679],
    "Argentina": [701], "Brazil": [710], "Bolivia": [720], "Chile": [725], "Colombia": [730], "Ecuador": [735],
    "Falkland Islands": [740], "French Guiana": [745], "Guyana": [750], "Paraguay": [755], "Peru": [760],
    "Suriname": [765], "Uruguay": [770], "Venezuela": [775],
}
MID = {mid: country for country, mids in _MID_RANGES.items() for mid in mids}

# Names that read "the flag of the ...".
_WITH_ARTICLE = {
    "United Kingdom", "Netherlands", "Faroe Islands", "Bahamas", "Cayman Islands", "Dominican Republic",
    "United States", "Turks and Caicos Islands", "British Virgin Islands", "US Virgin Islands", "Philippines",
    "United Arab Emirates", "Maldives", "Cook Islands", "Northern Mariana Islands", "Marshall Islands",
    "Solomon Islands", "Central African Republic", "Comoros", "Gambia", "Seychelles", "DR Congo",
    "Falkland Islands", "Vatican City",
}


def flag(mmsi: int) -> str | None:
    """Flag state from the MMSI: a ship's MMSI starts with its MID."""
    s = str(mmsi)
    if len(s) != 9:
        return None
    if s[:2] == "98":   # craft associated with a parent ship (lifeboat, tender)
        return MID.get(int(s[2:5]))
    if s[0] in "234567":
        return MID.get(int(s[:3]))
    return None


# Pilot vessels, tugs and towing vessels, search and rescue, law enforcement:
# service craft that wait or work over cables as part of their job. Must match
# the vessel_type labels the payload statements filter on (flink/40-42).
SERVICE_TYPES = {31, 32, 50, 51, 52, 55}


def is_service(code: int | None) -> bool:
    return code in SERVICE_TYPES


def ship_type(code: int | None) -> str | None:
    """AIS ship and cargo type (ITU-R M.1371), in words."""
    if code is None:
        return None
    named = {
        30: "fishing vessel", 31: "towing vessel", 32: "towing vessel", 33: "dredger", 34: "diving support vessel",
        35: "military vessel", 36: "sailing vessel", 37: "pleasure craft", 50: "pilot vessel",
        51: "search and rescue vessel", 52: "tug", 53: "port tender", 54: "anti-pollution vessel",
        55: "law enforcement vessel", 58: "medical transport", 59: "non-combatant ship",
    }
    if code in named:
        return named[code]
    decade = {2: "wing-in-ground craft", 4: "high-speed craft", 6: "passenger ship", 7: "cargo ship",
              8: "tanker", 9: "vessel"}.get(code // 10)
    if decade is None:
        return None
    # The second digit of 6x-8x marks dangerous cargo, category A (most hazardous) to D.
    hazard = {1: "A", 2: "B", 3: "C", 4: "D"}.get(code % 10) if code // 10 in (6, 7, 8) else None
    return f"{decade} carrying hazardous cargo (category {hazard})" if hazard else decade


def describe(mmsi: int, static: dict | None) -> str:
    """One or two plain sentences, e.g. "EBBA (IMO 7812555) is a passenger ship sailing under the flag of Sweden. ..."."""
    static = static or {}
    name = static.get("name") or f"MMSI {mmsi}"
    imo = f" (IMO {static['imo']})" if static.get("imo") else ""
    kind = ship_type(static.get("ship_type"))
    country = flag(mmsi)
    if country in _WITH_ARTICLE:
        country = f"the {country}"
    article = "an" if kind and kind[0] in "aeiou" else "a"
    if kind and country:
        sep = ", " if "hazardous" in kind else " "
        first = f"{name}{imo} is {article} {kind}{sep}sailing under the flag of {country}."
    elif kind:
        first = f"{name}{imo} is {article} {kind}; its flag state is not encoded in its MMSI."
    elif country:
        first = f"{name}{imo} is sailing under the flag of {country}; it has not broadcast its ship type."
    else:
        first = f"{name}{imo} has not broadcast its ship type or a flag-state MMSI."
    length, beam = static.get("length_m"), static.get("beam_m")
    if length and beam:
        return f"{first} Its length overall is about {length} metres and its beam {beam} metres."
    if length:
        return f"{first} Its length overall is about {length} metres."
    return first
