# 5010 Final Project: Black--Scholes Pricing and Dynamic Delta Hedging

This repository contains the code, data outputs, figures, and notebook documentation for the W5010 final project on Black--Scholes option pricing and dynamic delta hedging. The project studies how theoretical Black--Scholes prices and deltas behave when applied to realistic SPY option episodes, and how hedging performance changes under different volatility inputs, rebalancing frequencies, and transaction-cost assumptions.

## Project Overview

The main goal of the project is to connect the continuous-time Black--Scholes framework with a discrete empirical hedging simulation. The workflow has three major parts:

1. **Data preparation**: organize SPY price data, risk-free rates, and option episode definitions.
2. **Black--Scholes modeling**: compute theoretical option prices and deltas for each option episode across trading days.
3. **Dynamic hedging simulation**: use the Black--Scholes delta as the hedge ratio, rebalance the stock position over time, and evaluate hedging error and terminal P&L.

The final report and presentation use the saved tables and figures in `data/output/report_tables/` and `data/output/report_figures/`.

## Repository Structure

```text
5010-Project/
├── README.md
├── MATH 5010 Project Proposal.pdf
├── black_scholes_model.ipynb
├── black_scholes_model.pdf
├── black_scholes_model_UPDATED_JOB_C.ipynb
├── black_scholes_model_UPDATED_JOB_C.pdf
└── data/
    └── output/
        ├── episodes.parquet
        ├── stock_prices.parquet
        ├── risk_free.parquet
        ├── bs_delta_panel.parquet
        ├── bs_delta_panel.csv
        ├── hedge_performance_series.parquet
        ├── hedge_performance_series.csv
        ├── report_figures/
        └── report_tables/
```

## Main Notebooks

### `black_scholes_model.ipynb`

This notebook contains the original Black--Scholes modeling workflow. It loads the cleaned project data, constructs the option episode panel, and computes theoretical Black--Scholes values and deltas.

### `black_scholes_model_UPDATED_JOB_C.ipynb`

This is the updated modeling and hedging notebook. It extends the Black--Scholes panel into the dynamic delta-hedging layer. The notebook saves the main modeling and hedging outputs used in the final report.

Main outputs from this notebook include:

- `data/output/bs_delta_panel.parquet`
- `data/output/bs_delta_panel.csv`
- `data/output/hedge_performance_series.parquet`
- `data/output/hedge_performance_series.csv`
- `data/output/report_tables/hedge_summary.csv`
- `data/output/report_tables/hedge_rebalance_frequency_summary.csv`
- figures in `data/output/report_figures/`

## Data Inputs

The notebook expects the following cleaned input files inside `data/output/`:

| File | Description |
|---|---|
| `episodes.parquet` | Defines the option episodes, including strike, maturity, option type, and market regime. |
| `stock_prices.parquet` | Contains SPY daily price data and realized-volatility columns. |
| `risk_free.parquet` | Contains the daily risk-free rate series used in Black--Scholes pricing and cash-account calculations. |

These files are treated as the cleaned handoff data for the modeling and hedging stages.

## Main Modeling Outputs

| File | Description |
|---|---|
| `bs_delta_panel.parquet` / `bs_delta_panel.csv` | Main Black--Scholes modeling panel. One row represents one option episode, one trading day, and one volatility method. |
| `hedge_performance_series.parquet` / `hedge_performance_series.csv` | Main dynamic hedging panel. One row represents one option episode, volatility method, rebalancing rule, transaction-cost setting, and trading day. |
| `report_tables/bs_delta_summary.csv` | Summary of Black--Scholes price and delta behavior. |
| `report_tables/hedge_summary.csv` | Summary of hedging performance across volatility inputs and rebalancing assumptions. |
| `report_tables/hedge_rebalance_frequency_summary.csv` | Summary table comparing hedging error by rebalancing frequency. |

## Report Figures

The report figures are stored in `data/output/report_figures/`. Important files include:

| Figure File | Purpose |
|---|---|
| `fig_delta_paths.png` | Shows the Black--Scholes delta paths for the selected representative episode. |
| `fig_price_paths.png` | Compares underlying price behavior and modeled option values. |
| `fig_hedge_error_by_rebalance_frequency.png` | Compares hedging error under different rebalancing frequencies. |
| `fig_terminal_hedging_pnl_by_vol_input.png` | Compares terminal hedging P&L across volatility inputs. |
| `fig_mtm_error_over_time.png` | Shows mark-to-market hedging error over time. |
| `fig_turnover_transaction_cost.png` | Shows the relationship between hedge turnover and transaction cost. |
| `fig_episode_terminal_pnl_bar.png` | Summarizes terminal hedging P&L by episode. |
| `table_model_diagnostics.png` | Figure version of selected model diagnostics for presentation/report use. |
| `table_volatility_inputs.png` | Figure version of the volatility input summary. |

Additional presentation-style figures, such as `Effect of Rebalancing Frequency.png`, `Effect of Volatility Input.png`, `Theory vs. Real Performance.png`, and `Three Sources of the Theory-Practice Gap.png`, are also stored in the same folder.

## Methods Summary

The project uses the Black--Scholes framework with a continuous dividend-yield proxy. For each option episode, the notebook computes theoretical price and delta across trading days under several volatility specifications, including VIX-implied volatility, VIX3M-implied volatility, and realized volatility measures.

The dynamic hedging simulation then uses the Black--Scholes delta as the target hedge ratio. At each rebalancing date, the stock position is adjusted, the cash account is updated, and transaction costs are recorded when applicable. The simulation tracks both daily mark-to-market hedging error and terminal hedging P&L.

## Software Requirements

The project is written in Python using Jupyter Notebook. The main packages are:

```text
numpy
pandas
matplotlib
scipy
pyarrow
jupyter
```

`pyarrow` is required for reading and writing Parquet files with pandas.

## Reproducing the Outputs

To reproduce the modeling and hedging outputs:

1. Clone or download the repository.
2. Make sure the cleaned input files are available in `data/output/`.
3. Open `black_scholes_model_UPDATED_JOB_C.ipynb` in Jupyter Notebook, JupyterLab, VS Code, or Google Colab.
4. Run the notebook from top to bottom.
5. Check the saved outputs under:
   - `data/output/`
   - `data/output/report_tables/`
   - `data/output/report_figures/`

The notebook uses portable project paths, so it should work as long as the repository structure is preserved.

## Notes for Submission

For the final submission, the report should focus on interpretation, results, and conclusions. Detailed running instructions are better placed in this README rather than inside the report. Code outputs, generated tables, and additional robustness figures can be included in the appendix or submitted as supporting files if required by the instructor.

Suggested submission materials:

- Final report PDF
- Final presentation PDF
- Main notebook: `black_scholes_model_UPDATED_JOB_C.ipynb`
- Generated output tables and figures, if required
- This `README.md`

## Project Team

This repository was prepared for the W5010 Introduction to Mathematics of Finance final project. Team member names should be listed in the final report and presentation title page according to the course submission instructions.
