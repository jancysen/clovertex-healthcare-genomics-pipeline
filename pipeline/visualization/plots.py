import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

CONSUMPTION_DIR = "datalake/consumption"
PLOTS_DIR = os.path.join(CONSUMPTION_DIR, "plots")


def generate_visualizations():
    print("Generating visualizations...")

    os.makedirs(PLOTS_DIR, exist_ok=True)

    # -----------------------------------
    # Variant hotspots plot
    # -----------------------------------
    variant_file = os.path.join(
        CONSUMPTION_DIR,
        "variant_hotspots.parquet"
    )

    if os.path.exists(variant_file):
        df_variants = pd.read_parquet(variant_file)

        if not df_variants.empty:
            x_col = None

            if "total_variants" in df_variants.columns:
                x_col = "total_variants"
            elif "variant_count" in df_variants.columns:
                x_col = "variant_count"

            if x_col and "gene" in df_variants.columns:
                plt.figure(figsize=(8, 5))

                sns.scatterplot(
                    data=df_variants,
                    x=x_col,
                    y="gene"
                )

                plt.title("Genomics Variant Hotspots")
                plt.xlabel("Variant Count")
                plt.ylabel("Gene")

                plt.savefig(
                    os.path.join(
                        PLOTS_DIR,
                        "genomics_hotspots.png"
                    )
                )

                plt.close()

    # -----------------------------------
    # Patient summary plot
    # -----------------------------------
    patient_file = os.path.join(
        CONSUMPTION_DIR,
        "patient_summary.parquet"
    )

    if os.path.exists(patient_file):
        df_patients = pd.read_parquet(patient_file)

        if not df_patients.empty and "age" in df_patients.columns:
            plt.figure(figsize=(8, 5))

            sns.histplot(
                df_patients["age"],
                bins=20
            )

            plt.title("Patient Age Distribution")
            plt.savefig(
                os.path.join(
                    PLOTS_DIR,
                    "age_distribution.png"
                )
            )

            plt.close()

    # -----------------------------------
    # Create plots README
    # -----------------------------------
    readme_path = os.path.join(
        PLOTS_DIR,
        "plots_README.md"
    )

    with open(readme_path, "w") as f:
        f.write("# Plot Documentation\n\n")
        f.write("- genomics_hotspots.png\n")
        f.write("- age_distribution.png\n")

    print("Visualizations generated successfully.")