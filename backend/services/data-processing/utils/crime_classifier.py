CRIME_GROUPS = {
    "Homicide & Assault": [
        "murder", "manslaughter", "assault", "domestic violence", 
        "kidnapping", "abduction", "coercive control", "intimidation", "stalking"
    ],
    "Sexual Offences": [
        "sexual assault", "sexual touching", "pornography"
    ],
    "Robbery & Theft": [
        "robbery", "blackmail", "extortion", "break and enter", "stolen goods", 
        "theft", "steal", "fraud", "receiving stolen"
    ],
    "Property Damage": [
        "arson", "malicious damage", "vandalism"
    ],
    "Drug Offences": [
        "cocaine", "narcotics", "cannabis", "amphetamines", "ecstasy", 
        "dealing", "trafficking", "cultivating", "importing drugs", "drug possession"
    ],
    "Weapons & Public Order": [
        "firearm", "weapon", "trespass", "offensive conduct", 
        "offensive language", "riot", "criminal intent", "armed"
    ],
    "Justice & Regulatory": [
        "escape custody", "apprehended violence order", "avo", "breach bail", 
        "fail to appear", "resist officer", "hinder officer", "transport regulatory"
    ]
}

def classify_crime(text):
    """
    Scans the text for keywords and returns the highest matching crime category.
    Returns 'General Crime' if no specific match is found.
    """
    text_lower = text.lower()
    
    category_scores = {category: 0 for category in CRIME_GROUPS}
    
    for category, keywords in CRIME_GROUPS.items():
        for keyword in keywords:
            # Most common matching keyword
            category_scores[category] += text_lower.count(keyword)
            
    # Find the category with the highest score
    best_match = max(category_scores, key=category_scores.get)
    
    # If the highest score is 0, we default to General Crime
    if category_scores[best_match] == 0:
        return "General Crime"
        
    return best_match