import pandas as pd

def load_formulary_data(file_path, target_ndc):
    """
    Loads and filters formulary data for a specific NDC.
    Handles NDC format with or without leading zeros.
    """
    df = pd.read_csv(file_path, delimiter='|')
    # Convert target_ndc to string, ensuring it's properly formatted
    target_ndc = str(target_ndc).zfill(11)
    # Convert DataFrame NDC column to string and pad with zeros
    df['NDC'] = df['NDC'].astype(str).str.zfill(11)
    return df[df['NDC'] == target_ndc]

def analyze_ndc_stats(file_path, target_ndc):
    """
    Analyzes formulary statistics for a specific NDC code.
    
    Args:
        file_path (str): Path to the formulary file
        target_ndc (str): NDC code to analyze
        
    Returns:
        dict: Dictionary containing prior auth %, average tier, and step therapy %
    """
    ndc_data = load_formulary_data(file_path, target_ndc)
    
    if len(ndc_data) == 0:
        return {
            'ndc': target_ndc,
            'prior_auth_percent': 0,
            'avg_tier': 0,
            'step_therapy_percent': 0,
            'count': 0,
            'formularies': set()
        }
    
    # Calculate statistics
    prior_auth_percent = (ndc_data['PRIOR_AUTHORIZATION_YN'] == 'Y').mean() * 100
    avg_tier = ndc_data['TIER_LEVEL_VALUE'].mean()
    step_therapy_percent = (ndc_data['STEP_THERAPY_YN'] == 'Y').mean() * 100
    
    return {
        'ndc': target_ndc,
        'prior_auth_percent': round(prior_auth_percent, 2),
        'avg_tier': round(avg_tier, 2),
        'step_therapy_percent': round(step_therapy_percent, 2),
        'count': len(ndc_data),
        'formularies': set(ndc_data['FORMULARY_ID'].unique())
    }

def get_current_requirements(data):
    """
    Calculate current requirements from formulary data
    """
    if len(data) == 0:
        return None
        
    return {
        'avg_tier': round(data['TIER_LEVEL_VALUE'].mean(), 2),
        'prior_auth_percent': round((data['PRIOR_AUTHORIZATION_YN'] == 'Y').mean() * 100, 2),
        'step_therapy_percent': round((data['STEP_THERAPY_YN'] == 'Y').mean() * 100, 2)
    }

def compare_formulary_periods(old_file, new_file, target_ndc):
    """
    Compares formulary data between two time periods for a specific NDC.
    
    Args:
        old_file (str): Path to the older formulary file
        new_file (str): Path to the newer formulary file
        target_ndc (str): NDC code to analyze
        
    Returns:
        dict: Comparison statistics between the two periods
    """
    # Load data from both periods
    old_data = load_formulary_data(old_file, target_ndc)
    new_data = load_formulary_data(new_file, target_ndc)
    
    # Get sets of formulary IDs
    old_formularies = set(old_data['FORMULARY_ID'].unique())
    new_formularies = set(new_data['FORMULARY_ID'].unique())
    
    # Calculate formulary changes
    added_formularies = new_formularies - old_formularies
    removed_formularies = old_formularies - new_formularies
    maintained_formularies = old_formularies.intersection(new_formularies)
    
    # Calculate coverage percentages
    old_total_formularies = len(pd.read_csv(old_file, delimiter='|')['FORMULARY_ID'].unique())
    new_total_formularies = len(pd.read_csv(new_file, delimiter='|')['FORMULARY_ID'].unique())
    
    # For maintained formularies, calculate metric changes
    changes = {
        'tier_changes': [],
        'prior_auth_changes': [],
        'step_therapy_changes': []
    }
    
    for formulary_id in maintained_formularies:
        old_form = old_data[old_data['FORMULARY_ID'] == formulary_id].iloc[0]
        new_form = new_data[new_data['FORMULARY_ID'] == formulary_id].iloc[0]
        
        changes['tier_changes'].append(new_form['TIER_LEVEL_VALUE'] - old_form['TIER_LEVEL_VALUE'])
        changes['prior_auth_changes'].append(new_form['PRIOR_AUTHORIZATION_YN'] != old_form['PRIOR_AUTHORIZATION_YN'])
        changes['step_therapy_changes'].append(new_form['STEP_THERAPY_YN'] != old_form['STEP_THERAPY_YN'])
    
    # Get current requirements for both periods
    old_requirements = get_current_requirements(old_data)
    new_requirements = get_current_requirements(new_data)
    
    return {
        'ndc': target_ndc,
        'formulary_changes': {
            'added': len(added_formularies),
            'removed': len(removed_formularies),
            'maintained': len(maintained_formularies),
            'added_list': list(added_formularies),
            'removed_list': list(removed_formularies)
        },
        'metric_changes': {
            'avg_tier_change': round(sum(changes['tier_changes']) / len(maintained_formularies), 2) if maintained_formularies else 0,
            'prior_auth_changes': sum(changes['prior_auth_changes']),
            'step_therapy_changes': sum(changes['step_therapy_changes'])
        },
        'coverage': {
            'old_coverage_percent': round(len(old_formularies) / old_total_formularies * 100, 2),
            'new_coverage_percent': round(len(new_formularies) / new_total_formularies * 100, 2)
        },
        'current_requirements': {
            'old': old_requirements,
            'new': new_requirements
        }
    }

if __name__ == "__main__":
    # File paths
    old_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2023-12\2024_20250131_Dec2023\basic drugs formulary file  20231231\basic drugs formulary file  20231231.txt"
    new_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-02\2025_20250220\basic drugs formulary file  20250228\basic drugs formulary file  20250228.txt"
    
    # Example NDC (trying a different one from your sample data)
    test_ndc = '00069197540'
    
    # Perform comparison
    comparison = compare_formulary_periods(old_file, new_file, test_ndc)
    
    # Print results
    print(f"\nFormulary Comparison for NDC {comparison['ndc']}:")
    print("\n1. Formulary Changes:")
    print(f"• Added: {comparison['formulary_changes']['added']} formularies")
    print(f"• Removed: {comparison['formulary_changes']['removed']} formularies")
    print(f"• Maintained: {comparison['formulary_changes']['maintained']} formularies")
    
    coverage_change = comparison['coverage']['new_coverage_percent'] - comparison['coverage']['old_coverage_percent']
    coverage_trend = "↑" if coverage_change > 0 else "↓" if coverage_change < 0 else "→"
    
    print("\n2. Coverage Analysis:")
    print(f"• December 2024: {comparison['coverage']['old_coverage_percent']}% of formularies")
    print(f"• February 2025: {comparison['coverage']['new_coverage_percent']}% of formularies")
    print(f"• Trend: {coverage_trend} {abs(coverage_change):.2f}% change")
    
    print("\n3. Current Requirements and Changes:")
    if comparison['current_requirements']['new']:
        new_reqs = comparison['current_requirements']['new']
        print("Current Status (February 2025):")
        print(f"• Average Tier Level: {new_reqs['avg_tier']}")
        print(f"• Prior Authorization Required: {new_reqs['prior_auth_percent']}% of formularies")
        print(f"• Step Therapy Required: {new_reqs['step_therapy_percent']}% of formularies")
        
        print("\nChanges from December 2024:")
        maintained = comparison['formulary_changes']['maintained']
        if maintained > 0:
            avg_tier = comparison['metric_changes']['avg_tier_change']
            tier_trend = "increased" if avg_tier > 0 else "decreased" if avg_tier < 0 else "unchanged"
            print(f"• Average tier {tier_trend} by {abs(avg_tier):.1f}")
            
            pa_changes = comparison['metric_changes']['prior_auth_changes']
            if pa_changes > 0:
                print(f"• Prior Authorization requirements changed for {pa_changes} formularies ({(pa_changes/maintained)*100:.1f}%)")
            
            st_changes = comparison['metric_changes']['step_therapy_changes']
            if st_changes > 0:
                print(f"• Step Therapy requirements changed for {st_changes} formularies ({(st_changes/maintained)*100:.1f}%)")
            
            if pa_changes == 0 and st_changes == 0:
                print("• No changes in Prior Authorization or Step Therapy requirements")
    
    # Display some of the specific formularies if there were changes
    if comparison['formulary_changes']['added'] > 0:
        print("\nExample Added Formularies (up to 5):")
        for f in comparison['formulary_changes']['added_list'][:5]:
            print(f"• {f}")
            
    if comparison['formulary_changes']['removed'] > 0:
        print("\nExample Removed Formularies (up to 5):")
        for f in comparison['formulary_changes']['removed_list'][:5]:
            print(f"• {f}")
