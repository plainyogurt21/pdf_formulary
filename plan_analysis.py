import pandas as pd

def load_plans_data(plan_file):
    """
    Loads plan information and creates a mapping of formulary ID to plan details
    """
    # Try different encodings
    encodings = ['utf-8', 'latin1', 'cp1252']
    
    for encoding in encodings:
        try:
            plans_df = pd.read_csv(plan_file, delimiter='|', encoding=encoding, low_memory=False)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error loading file with {encoding} encoding: {str(e)}")
            continue
    # Create a unique plan identifier combining contract and plan ID
    plans_df['PLAN_KEY'] = plans_df['CONTRACT_ID'] + '_' + plans_df['PLAN_ID'].astype(str)
    return plans_df

def load_formulary_with_plans(formulary_file, plan_file, target_ndc):
    """
    Loads and joins formulary data with plan information for a specific NDC
    """
    # Load formulary data
    # Try different encodings
    encodings = ['utf-8', 'latin1', 'cp1252']
    
    for encoding in encodings:
        try:
            formulary_df = pd.read_csv(formulary_file, delimiter='|', encoding=encoding, low_memory=False)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error loading file with {encoding} encoding: {str(e)}")
            continue
    formulary_df = formulary_df[formulary_df['NDC'].astype(str).str.zfill(11) == str(target_ndc).zfill(11)]
    
    # Load and join plan data
    plans_df = load_plans_data(plan_file)
    
    # Merge formulary data with plan information
    merged_df = pd.merge(
        formulary_df,
        plans_df,
        on='FORMULARY_ID',
        how='inner'
    ).drop_duplicates('PLAN_KEY')  # Keep only unique plans
    
    return merged_df

def analyze_plan_changes(old_formulary_file, new_formulary_file, old_plan_file, new_plan_file, target_ndc):
    """
    Analyzes changes in plan coverage between two time periods for a specific NDC
    Also returns total unique plan counts for each file
    """
    # Load total plan counts first
    old_total_plans = len(load_plans_data(old_plan_file)['PLAN_KEY'].unique())
    new_total_plans = len(load_plans_data(new_plan_file)['PLAN_KEY'].unique())
    
    # Load data for both periods
    old_data = load_formulary_with_plans(old_formulary_file, old_plan_file, target_ndc)
    new_data = load_formulary_with_plans(new_formulary_file, new_plan_file, target_ndc)
    
    # Get unique plan identifiers
    old_plans = set(old_data['PLAN_KEY'])
    new_plans = set(new_data['PLAN_KEY'])
    
    # Calculate plan changes
    added_plans = new_plans - old_plans
    removed_plans = old_plans - new_plans
    maintained_plans = old_plans.intersection(new_plans)
    
    # Get details for changed plans
    added_plan_details = new_data[new_data['PLAN_KEY'].isin(added_plans)][
        ['PLAN_KEY', 'CONTRACT_NAME', 'PLAN_NAME']
    ].to_dict('records')
    
    removed_plan_details = old_data[old_data['PLAN_KEY'].isin(removed_plans)][
        ['PLAN_KEY', 'CONTRACT_NAME', 'PLAN_NAME']
    ].to_dict('records')
    
    def calculate_metrics(df):
        """Calculate tier, PA, and ST metrics for a given DataFrame"""
        if len(df) == 0:
            return {
                'avg_tier': 0,
                'pa_percent': 0,
                'st_percent': 0
            }
        return {
            'avg_tier': df['TIER_LEVEL_VALUE'].sum() / len(df),
            'pa_percent': (df['PRIOR_AUTHORIZATION_YN'] == 'Y').sum() / len(df) * 100,
            'st_percent': (df['STEP_THERAPY_YN'] == 'Y').sum() / len(df) * 100
        }
    
    # Calculate metrics for all plans
    old_metrics = calculate_metrics(old_data)
    new_metrics = calculate_metrics(new_data)
    
    # Calculate metrics for maintained plans
    if maintained_plans:
        old_maintained = old_data[old_data['PLAN_KEY'].isin(maintained_plans)].set_index('PLAN_KEY')
        new_maintained = new_data[new_data['PLAN_KEY'].isin(maintained_plans)].set_index('PLAN_KEY')
        
        # Sort for alignment
        old_maintained = old_maintained.sort_index()
        new_maintained = new_maintained.sort_index()
        
        maintained_old_metrics = calculate_metrics(old_maintained)
        maintained_new_metrics = calculate_metrics(new_maintained)
        
        # Calculate changes for maintained plans
        pa_changes = (new_maintained['PRIOR_AUTHORIZATION_YN'] != old_maintained['PRIOR_AUTHORIZATION_YN'])
        st_changes = (new_maintained['STEP_THERAPY_YN'] != old_maintained['STEP_THERAPY_YN'])
    else:
        maintained_old_metrics = {'avg_tier': 0, 'pa_percent': 0, 'st_percent': 0}
        maintained_new_metrics = {'avg_tier': 0, 'pa_percent': 0, 'st_percent': 0}
        pa_changes = pd.Series()
        st_changes = pd.Series()
    
    # Calculate metrics for added plans
    added_metrics = calculate_metrics(new_data[new_data['PLAN_KEY'].isin(added_plans)])
        
    metric_changes = {
        'all_plans': {
            'old': {
                'avg_tier': round(old_metrics['avg_tier'], 1),
                'pa_percent': round(old_metrics['pa_percent'], 1),
                'st_percent': round(old_metrics['st_percent'], 1)
            },
            'new': {
                'avg_tier': round(new_metrics['avg_tier'], 1),
                'pa_percent': round(new_metrics['pa_percent'], 1),
                'st_percent': round(new_metrics['st_percent'], 1)
            }
        },
        'maintained_plans': {
            'old': {
                'avg_tier': round(maintained_old_metrics['avg_tier'], 1),
                'pa_percent': round(maintained_old_metrics['pa_percent'], 1),
                'st_percent': round(maintained_old_metrics['st_percent'], 1)
            },
            'new': {
                'avg_tier': round(maintained_new_metrics['avg_tier'], 1),
                'pa_percent': round(maintained_new_metrics['pa_percent'], 1),
                'st_percent': round(maintained_new_metrics['st_percent'], 1)
            }
        },
        'added_plans': {
            'avg_tier': round(added_metrics['avg_tier'], 1),
            'pa_percent': round(added_metrics['pa_percent'], 1),
            'st_percent': round(added_metrics['st_percent'], 1)
        },
        'changes': {
            'prior_auth_changes': pa_changes.sum(),
            'step_therapy_changes': st_changes.sum()
        }
    }
    
    return {
        'ndc': target_ndc,
        'plan_changes': {
            'added': len(added_plans),
            'removed': len(removed_plans),
            'maintained': len(maintained_plans),
            'added_details': added_plan_details,
            'removed_details': removed_plan_details
        },
        'metric_changes': metric_changes,
        'coverage': {
            'old_coverage_percent': round(len(old_plans) / old_total_plans * 100, 2),
            'new_coverage_percent': round(len(new_plans) / new_total_plans * 100, 2)
        },
        'total_plans': {
            'old': old_total_plans,
            'new': new_total_plans
        }
    }

if __name__ == "__main__":
    # File paths
    old_formulary_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2023-12\2024_20250131_Dec2023\basic drugs formulary file  20231231\basic drugs formulary file  20231231.txt"
    old_formulary_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-02\2024_20250228_Feb2024\basic drugs formulary file  20240229\basic drugs formulary file  20240229.txt"

    new_formulary_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-02\2025_20250220\basic drugs formulary file  20250228\basic drugs formulary file  20250228.txt"
    old_plan_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2023-12\2024_20250131_Dec2023\plan information  20231231\plan information  20231231.txt"
    old_plan_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-02\2024_20250228_Feb2024\plan information  20240229\plan information  20240229.txt"
    new_plan_file = r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-02\2025_20250220\plan information  20250228\plan information  20250228.txt"
    
    # Example NDCs
    test_ndcs = ['00069197540',  #tafa
                 '00069197530', #Tafa
                 '72511075001', #REpatha
                  '66302030001', #orenetram
                 '66302061002', #Tyvaso DPI
                  '00169413013',#Ozempic
                  '73625011111', #Mavacamtem
                '70370106001', #Ingrezza,
                '61958250101' #Biktavrvy
                  ] 
    
    print("\n" + "="*80)
    print("FORMULARY COVERAGE ANALYSIS")
    print("="*80 + "\n")
    
    # First show total plan counts
    old_total = len(load_plans_data(old_plan_file)['PLAN_KEY'].unique())
    new_total = len(load_plans_data(new_plan_file)['PLAN_KEY'].unique())
    print(f"Total Unique Plans:")
    print(f"• Old February 2024: {old_total} plans")
    print(f"• February 2025: {new_total} plans")
    print("\n" + "="*80)
    
    # Analyze each NDC
    for ndc in test_ndcs:
        # Perform comparison
        comparison = analyze_plan_changes(
            old_formulary_file, new_formulary_file,
            old_plan_file, new_plan_file,
            ndc
        )
        
        # Print results
        print(f"\nPlan Coverage Comparison for NDC {comparison['ndc']}:")
        print("\n1. Plan Changes:")
        print(f"• Added: {comparison['plan_changes']['added']} plans")
        print(f"• Removed: {comparison['plan_changes']['removed']} plans")
        print(f"• Maintained: {comparison['plan_changes']['maintained']} plans")
        
        coverage_change = comparison['coverage']['new_coverage_percent'] - comparison['coverage']['old_coverage_percent']
        coverage_trend = "↑" if coverage_change > 0 else "↓" if coverage_change < 0 else "→"
        
        print("\n2. Coverage Analysis:")
        print(f"• December 2023: {comparison['coverage']['old_coverage_percent']}% of plans")
        print(f"• February 2025: {comparison['coverage']['new_coverage_percent']}% of plans")
        print(f"• Trend: {coverage_trend} {abs(coverage_change):.2f}% change")
        
        print("\n3. Drug Requirements Analysis:")
        
        # All Plans
        print("\nAll Plans Metrics:")
        old_m = comparison['metric_changes']['all_plans']['old']
        new_m = comparison['metric_changes']['all_plans']['new']
        
        tier_change = new_m['avg_tier'] - old_m['avg_tier']
        tier_trend = "increased" if tier_change > 0 else "decreased" if tier_change < 0 else "unchanged"
        print(f"• Average Tier: {tier_trend} by {abs(tier_change):.1f} ({old_m['avg_tier']:.1f} → {new_m['avg_tier']:.1f})")
        
        pa_change = new_m['pa_percent'] - old_m['pa_percent']
        pa_trend = "increased" if pa_change > 0 else "decreased" if pa_change < 0 else "unchanged"
        print(f"• Prior Authorization: {pa_trend} by {abs(pa_change):.1f}% ({old_m['pa_percent']:.1f}% → {new_m['pa_percent']:.1f}%)")
        
        st_change = new_m['st_percent'] - old_m['st_percent']
        st_trend = "increased" if st_change > 0 else "decreased" if st_change < 0 else "unchanged"
        print(f"• Step Therapy: {st_trend} by {abs(st_change):.1f}% ({old_m['st_percent']:.1f}% → {new_m['st_percent']:.1f}%)")
        
        # Maintained Plans
        maintained_count = comparison['plan_changes']['maintained']
        if maintained_count > 0:
            print("\nMaintained Plans Metrics:")
            old_m = comparison['metric_changes']['maintained_plans']['old']
            new_m = comparison['metric_changes']['maintained_plans']['new']
            
            tier_change = new_m['avg_tier'] - old_m['avg_tier']
            tier_trend = "increased" if tier_change > 0 else "decreased" if tier_change < 0 else "unchanged"
            print(f"• Average Tier: {tier_trend} by {abs(tier_change):.1f} ({old_m['avg_tier']:.1f} → {new_m['avg_tier']:.1f})")
            
            pa_change = new_m['pa_percent'] - old_m['pa_percent']
            pa_trend = "increased" if pa_change > 0 else "decreased" if pa_change < 0 else "unchanged"
            print(f"• Prior Authorization: {pa_trend} by {abs(pa_change):.1f}% ({old_m['pa_percent']:.1f}% → {new_m['pa_percent']:.1f}%)")
            
            st_change = new_m['st_percent'] - old_m['st_percent']
            st_trend = "increased" if st_change > 0 else "decreased" if st_change < 0 else "unchanged"
            print(f"• Step Therapy: {st_trend} by {abs(st_change):.1f}% ({old_m['st_percent']:.1f}% → {new_m['st_percent']:.1f}%)")
        
        # Added Plans
        if comparison['plan_changes']['added'] > 0:
            print("\nNewly Added Plans Metrics:")
            m = comparison['metric_changes']['added_plans']
            print(f"• Average Tier: {m['avg_tier']:.1f}")
            print(f"• Prior Authorization: {m['pa_percent']:.1f}%")
            print(f"• Step Therapy: {m['st_percent']:.1f}%")
    
        
        print("\n" + "="*80)
