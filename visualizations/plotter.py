import matplotlib.pyplot as plt

def visualize_results(df, x_axis, y_axises):
    if df is None or df.empty:
        print("No data to visualize.")
        return

    # Sort the DataFrame by the x-axis column
    df = df.sort_values(by=x_axis)

    plt.figure(figsize=(12, 8))
    for y_col in y_axises:
        if y_col in df.columns:
            y_values = df[y_col].astype(float)
            # Plot the line
            plt.plot(df[x_axis], y_values, marker='o', linestyle='-', label=y_col)
            # Highlight points where the value is 400
            for i, value in enumerate(y_values):
                if value == 400:
                    plt.scatter(df[x_axis].iloc[i], value, color='red', zorder=5)

    plt.xlabel(x_axis)
    plt.ylabel(", ".join(y_axises))
    plt.title(f'{x_axis} vs {", ".join(y_axises)}')
    plt.xticks(rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()
    plt.grid(True)
    plt.show()