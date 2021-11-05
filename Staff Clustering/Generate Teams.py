import pandas as pd
import numpy as np
from math import ceil, floor
from random import choice
import matplotlib.pyplot as plt
import seaborn as sns
import argparse


def load_and_process_data(path):
    history = pd.read_excel(path, sheet_name='Sheet7').dropna() # Sheet 7 contains pivot table 
    history['Team'] = float('nan')  
    try:
        roles = pd.read_excel(path, sheet_name='Individual_Staffing_8wk_report_', usecols=['User', 'User Job code v2']).drop_duplicates().reset_index(drop=True) # ensure roles are on sheet Individual_Staffing_8wk_report_ in column Job Code v2 
    except ValueError:
        raise Exception('Ensure "Individual_Staffing_8wk_report_" sheet has "User" and "User Job code v2" columns!')
    roles = roles.rename(columns={'User Job code v2': 'User Job code'})
    
    # order of consulting roles
    role_order = ['Intern', 'Operations', 'Architect', 'Analyst', 'Consultant', 'Senior Consultant', 'Manager', 'Director', 'VP', 'Managing Director']
    roles['User Job code'] = roles['User Job code'].astype('category').cat.reorder_categories(role_order).cat.as_ordered()

    # merge pivot table with roles
    history = history.merge(roles, how='left', left_on='Employee', right_on='User')
    roles = history['User Job code']
    history = history.drop(labels=['User Job code', 'User'], axis=1)
    history.insert(2, 'Role', roles)
    
    leads = pd.read_excel(path, sheet_name='Leads', header=None) # Sheet Leads contains leads
    leads = leads.values[:,0].tolist()
    leads = [', '.join(lead.split(' ')[::-1]) for lead in leads]
    
    return history, roles, leads
    

def assign_teams_to_leads(history, leads):
    for group_num, lead in enumerate(leads):
        lead_vec = history[history['Employee'] == lead].iloc[:, 3:-1]
        history.loc[lead_vec.index, 'Team'] = group_num + 1
    return history


def generate_teams(history, selection_method, num_teams, round_funcs = [floor, ceil]):
    for group_num in range(1, num_teams + 1):
        group_avg_vec = history[history['Team'] == group_num].iloc[:, 3:-1].values.mean(axis=0)
        for role in history['Role'].cat.categories[::-1]: # loop through MDs, VPs, Directors, Managers, Senior Consultants, Analysts, and others   
            if group_num < num_teams:
                if selection_method == 'random':
                    func = choice(round_funcs)
                    num_mbrs_from_role_in_group = func((history['Role'] == role).sum() / num_teams)
                elif selection_method == 'alternating':
                    num_mbrs_from_role_in_group = round_funcs[-1]((history['Role'] == role).sum() / num_teams)
                    round_funcs.insert(0, round_funcs.pop())
                else:
                    raise Exception('Invalid SELECTION_METHOD! Options are "random" or "alternating".')
            else:
                num_mbrs_from_role_in_group = history[(history['Role'] == role) & (history['Team'].isna())].shape[0]
            data_vecs = history[(history['Role'] == role) & (history['Team'].isna())].iloc[:, 3:-1]
            dist = pd.Series(np.linalg.norm(np.subtract(group_avg_vec, data_vecs.values), axis=1), index=data_vecs.index).sort_values()
            history.loc[dist.iloc[:num_mbrs_from_role_in_group].index, 'Team'] = group_num
    return history


def save_team_dist(history, save_path):
    team_splits = history.groupby('Team')['Role'].value_counts()
    team_splits.name = None
    team_splits = team_splits.reset_index().rename(columns={0: 'Count'})
    team_splits['Team'] = team_splits['Team'].astype(int)
    team_splits['Role'] = team_splits['Role']
    
    fig, ax = plt.subplots(figsize=(15, 4))
    sns.barplot(x='Team', y='Count', hue='Role', data=team_splits)
    ax.legend(loc='upper left', bbox_to_anchor=(0.75, 1.35), ncol=3, fancybox=True, shadow=True)
    plt.title('Team Distributions')
    fig.savefig(f'{save_path}/team_dist.png', bbox_inches='tight')
    return fig
    
    
def export(history, save_path):
    history.to_csv(f'{save_path}/assignments_greedy.csv')

    
def configCLIParser(parser):
    parser.add_argument("--data_path", help='Path to excel sheet containing "Sheet7" containing project history, "Individual_Staffing_8wk_report_" containing roles, and "Leads" containing team leaders.', type=str)
    parser.add_argument("--selection_method", help="Method to use in greedy algorithm when selecting teams.", default = "random", choices = ['random', 'alternating'])
    parser.add_argument("--save_path", help="Directory for saving team assignments and distribution plot.", default = ".")
    return parser

def main(args):
    history, roles, leads = load_and_process_data(args.data_path)
    history = assign_teams_to_leads(history, leads)
    history = generate_teams(history, args.selection_method, len(leads))
    save_team_dist(history, args.save_path)
    export(history, args.save_path)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser() # Create module's cli parser.
    parser = configCLIParser(parser)
    args = parser.parse_args()
    
    main(args)
