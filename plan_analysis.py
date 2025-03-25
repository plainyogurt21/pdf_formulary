import pandas as pd

def load_plans_data(plan_file):
    """
    Loads plan information and creates a mapping of formulary ID to plan details.
    Returns a tuple: (plans_df, total_plans_count).
    """
    # Try different encodings
    encodings = ['utf-8', 'latin1', 'cp1252']
    plans_df = None
    
    for encoding in encodings:
        try:
            plans_df = pd.read_csv(plan_file, delimiter='|', encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error loading file with {encoding} encoding: {str(e)}")
            continue

    # Create a unique plan identifier combining contract and plan ID
    plans_df['PLAN_KEY'] = plans_df['CONTRACT_ID'].astype(str) + '_' + plans_df['PLAN_ID'].astype(str)

    total_plans_count = plans_df['PLAN_KEY'].nunique()
    return plans_df, total_plans_count

def load_formulary_data(formulary_file):
    """
    Loads the entire formulary data for a given time period.
    Returns the raw formulary DataFrame.
    """
    encodings = ['utf-8', 'latin1', 'cp1252']
    formulary_df = None

    for encoding in encodings:
        try:
            formulary_df = pd.read_csv(formulary_file, delimiter='|', encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error loading file with {encoding} encoding: {str(e)}")
            continue
    
    return formulary_df

def analyze_plan_changes(
    old_formulary_df, old_plans_df, old_total_plans,
    new_formulary_df, new_plans_df, new_total_plans,
    target_ndc
):
    """
    Analyzes changes in plan coverage between two time periods for a specific NDC,
    using in-memory DataFrames rather than reading from disk.
    
    Returns a dictionary containing plan changes, metrics, and coverage information.
    """

    # Subset the formulary data for the specified NDC (zfill to ensure consistent 11-digit NDC)
    target_ndc_str = str(target_ndc).zfill(11)
    old_data = old_formulary_df[old_formulary_df['NDC'].astype(str).str.zfill(11) == target_ndc_str]
    new_data = new_formulary_df[new_formulary_df['NDC'].astype(str).str.zfill(11) == target_ndc_str]

    # Merge with plan information on FORMULARY_ID, then drop duplicates by PLAN_KEY
    old_merged = pd.merge(old_data, old_plans_df, on='FORMULARY_ID', how='inner').drop_duplicates('PLAN_KEY')
    new_merged = pd.merge(new_data, new_plans_df, on='FORMULARY_ID', how='inner').drop_duplicates('PLAN_KEY')

    # Get unique plan identifiers
    old_plans = set(old_merged['PLAN_KEY'])
    new_plans = set(new_merged['PLAN_KEY'])

    # Calculate plan changes
    added_plans = new_plans - old_plans
    removed_plans = old_plans - new_plans
    maintained_plans = old_plans.intersection(new_plans)

    # Prepare details for added/removed plans
    added_plan_details = new_merged[new_merged['PLAN_KEY'].isin(added_plans)][
        ['PLAN_KEY', 'CONTRACT_NAME', 'PLAN_NAME']
    ].to_dict('records')
    
    removed_plan_details = old_merged[old_merged['PLAN_KEY'].isin(removed_plans)][
        ['PLAN_KEY', 'CONTRACT_NAME', 'PLAN_NAME']
    ].to_dict('records')

    def calculate_metrics(df):
        """Calculate tier, PA, and ST metrics for a given DataFrame"""
        if len(df) == 0:
            return {
                'avg_tier': 0.0,
                'pa_percent': 0.0,
                'st_percent': 0.0
            }
        return {
            'avg_tier': df['TIER_LEVEL_VALUE'].sum() / len(df),
            'pa_percent': (df['PRIOR_AUTHORIZATION_YN'] == 'Y').sum() / len(df) * 100,
            'st_percent': (df['STEP_THERAPY_YN'] == 'Y').sum() / len(df) * 100
        }

    # Calculate metrics for all plans that cover the drug
    old_metrics = calculate_metrics(old_merged)
    new_metrics = calculate_metrics(new_merged)

    # Calculate metrics for maintained plans
    if maintained_plans:
        old_maintained = old_merged[old_merged['PLAN_KEY'].isin(maintained_plans)].set_index('PLAN_KEY').sort_index()
        new_maintained = new_merged[new_merged['PLAN_KEY'].isin(maintained_plans)].set_index('PLAN_KEY').sort_index()

        maintained_old_metrics = calculate_metrics(old_maintained)
        maintained_new_metrics = calculate_metrics(new_maintained)

        pa_changes = (new_maintained['PRIOR_AUTHORIZATION_YN'] != old_maintained['PRIOR_AUTHORIZATION_YN'])
        st_changes = (new_maintained['STEP_THERAPY_YN'] != old_maintained['STEP_THERAPY_YN'])
    else:
        maintained_old_metrics = {'avg_tier': 0, 'pa_percent': 0, 'st_percent': 0}
        maintained_new_metrics = {'avg_tier': 0, 'pa_percent': 0, 'st_percent': 0}
        pa_changes = pd.Series([], dtype=bool)
        st_changes = pd.Series([], dtype=bool)

    # Calculate metrics for added plans
    added_data = new_merged[new_merged['PLAN_KEY'].isin(added_plans)]
    added_metrics = calculate_metrics(added_data)

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

    # Coverage = fraction of total plans that cover the drug
    old_coverage_percent = (len(old_plans) / old_total_plans * 100) if old_total_plans > 0 else 0
    new_coverage_percent = (len(new_plans) / new_total_plans * 100) if new_total_plans > 0 else 0

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
            'old_coverage_percent': round(old_coverage_percent, 2),
            'new_coverage_percent': round(new_coverage_percent, 2)
        },
        'total_plans': {
            'old': old_total_plans,
            'new': new_total_plans
        }
    }

def collect_metrics_by_period(period_data, time_periods, drug_mapping):
    """
    Collects comparison data into a DataFrame for plotting analysis.
    """
    # Create lists to store the metrics for each time period and drug
    metrics_data = []
    
    # For each drug
    for drug_name, ndcs in drug_mapping.items():
        for ndc in ndcs:
            # For each consecutive pair of time periods
            for i in range(len(time_periods) - 1):
                old_period = time_periods[i]
                new_period = time_periods[i + 1]
                
                # Get data for old and new periods
                old_data = period_data[old_period]
                new_data = period_data[new_period]
                
                # Analyze changes between periods
                comparison = analyze_plan_changes(
                    old_data['formulary_df'], old_data['plans_df'], old_data['total_plans'],
                    new_data['formulary_df'], new_data['plans_df'], new_data['total_plans'],
                    ndc
                )
                
                # Extract metrics for the new period
                metrics = {
                    'drug': drug_name,
                    'ndc': ndc,
                    'period': new_period,
                    'coverage': comparison['coverage']['new_coverage_percent'],
                    'total_plans': comparison['total_plans']['new'],
                    'maintained_plans': comparison['plan_changes']['maintained'],
                    'added_plans': comparison['plan_changes']['added'],
                    'removed_plans': comparison['plan_changes']['removed'],
                    'avg_tier': comparison['metric_changes']['all_plans']['new']['avg_tier'],
                    'pa_percent': comparison['metric_changes']['all_plans']['new']['pa_percent'],
                    'st_percent': comparison['metric_changes']['all_plans']['new']['st_percent']
                }
                
                # Also include metrics for the first period
                if i == 0:
                    first_metrics = {
                        'drug': drug_name,
                        'ndc': ndc,
                        'period': old_period,
                        'coverage': comparison['coverage']['old_coverage_percent'],
                        'total_plans': comparison['total_plans']['old'],
                        'maintained_plans': 0,  # No previous period to maintain from
                        'added_plans': 0,      # No previous period to add from
                        'removed_plans': 0,    # No previous period to remove from
                        'avg_tier': comparison['metric_changes']['all_plans']['old']['avg_tier'],
                        'pa_percent': comparison['metric_changes']['all_plans']['old']['pa_percent'],
                        'st_percent': comparison['metric_changes']['all_plans']['old']['st_percent']
                    }
                    metrics_data.append(first_metrics)
                
                metrics_data.append(metrics)
    
    # Convert to DataFrame
    metrics_df = pd.DataFrame(metrics_data)
    
    # Sort by drug name, NDC, and period for consistent ordering
    metrics_df = metrics_df.sort_values(['drug', 'ndc', 'period'])
    
    return metrics_df

if __name__ == "__main__":
    # Define file paths in a dictionary
    file_paths = {
        'feb2023': {
            'formulary': r"E:\Plan_Formulary_Files\2-2023\basic drugs formulary file  20230228.txt",
            'plan': r"E:\Plan_Formulary_Files\2-2023\plan information  20230228.txt"
        },
        'dec2023': {
            'formulary': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2023-12\2024_20250131_Dec2023\basic drugs formulary file  20231231\basic drugs formulary file  20231231.txt",
            'plan': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2023-12\2024_20250131_Dec2023\plan information  20231231\plan information  20231231.txt"
        },
        'dec2024': {
            'formulary': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-12\2025_20241212\basic drugs formulary file  20241231\basic drugs formulary file  20241231.txt",
            'plan': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-12\2025_20241212\plan information  20241231\plan information  20241231.txt"
        },
        'feb2024': {
            'formulary': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-02\2024_20250228_Feb2024\basic drugs formulary file  20240229\basic drugs formulary file  20240229.txt",
            'plan': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2024-02\2024_20250228_Feb2024\plan information  20240229\plan information  20240229.txt"
        },
        'feb2025': {
            'formulary': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-02\2025_20250220\basic drugs formulary file  20250228\basic drugs formulary file  20250228.txt",
            'plan': r"E:\Extracted_Files\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information\2025-02\2025_20250220\plan information  20250228\plan information  20250228.txt"
        }
    }

    # Define drug mapping for better readability
    drug_mapping = {
        'Tafamidis': ['00069197540'],
        'Repatha': ['72511075001'],
        'Orenitram': ['66302030001'],
        'Tyvaso DPI': ['66302061002'],
        'Ozempic': ['00169413013'],
        'Mavacamten': ['73625011111'],
        'Ingrezza': ['70370106001'],
        'Biktarvy': ['61958250101']
    }

    # Define sequential time periods
    time_periods = ['feb2023', 'dec2023', 'feb2024', 'dec2024', 'feb2025']

    # ------------------------------------------------------------------------------
    # 1) LOAD EACH PERIOD'S DATA INTO MEMORY JUST ONCE
    # ------------------------------------------------------------------------------
    period_data = {}

    for period in time_periods:
        plan_file = file_paths[period]['plan']
        formulary_file = file_paths[period]['formulary']
        
        # Load plan data
        plans_df, total_plan_count = load_plans_data(plan_file)
        
        # Load formulary data
        formulary_df = load_formulary_data(formulary_file)
        
        # Store in dictionary
        period_data[period] = {
            'plans_df': plans_df,
            'total_plans': total_plan_count,
            'formulary_df': formulary_df
        }

    print("\n" + "="*100)
    print("SEQUENTIAL FORMULARY COVERAGE ANALYSIS")
    print("="*100 + "\n")

    # ------------------------------------------------------------------------------
    # 2) COLLECT METRICS INTO DATAFRAME
    # ------------------------------------------------------------------------------
    metrics_df = collect_metrics_by_period(period_data, time_periods, drug_mapping)
    
    # Display metrics summary
    print("\nMetrics Summary:")
    print(metrics_df.head())
    
    # Save to CSV for future plotting
    metrics_df.to_csv('formulary_metrics.csv', index=False)
    print("\nMetrics saved to formulary_metrics.csv")
