import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px

# Assuming you have your test execution summary DataFrame stored in a CSV file
test_summary_df = pd.read_csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\summary.csv")


# Initialize Dash app
app = dash.Dash(__name__)

# Define the layout of the dashboard
app.layout = html.Div([
    html.H1("Historical Test Execution Dashboard"),

    # Bar chart to show validation type-wise pass/fail counts
    dcc.Graph(
        id='validation-type-pass-fail',
        figure=px.bar(test_summary_df, x='Source_name',
                      y='Number_of_failed_Records', color='Status',
                      barmode='stack',
                      title='Validation Type-wise Pass/Fail Counts')
    ),

    # Scatter plot to show source vs target records with pass/fail status
    dcc.Graph(
        id='source-target-records',
        figure=px.scatter(test_summary_df, x='Number_of_source_Records',
                          y='Number_of_target_Records', color='Status',
                          size='Number_of_failed_Records',
                          hover_data=['batch_id', 'column'],
                          title='Source vs Target Records with Pass/Fail Status')
    )
])

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
