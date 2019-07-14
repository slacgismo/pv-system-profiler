import seaborn as sns
import matplotlib.pyplot as plt

def heatmap(power_signals, pltname):
    with sns.axes_style("white"):
        plt.figure(figsize=(12,4))
        foo = plt.imshow(power_signals, cmap='hot', interpolation='none', aspect='auto')
        plt.title('Measured power')
        plt.colorbar(foo)
        plt.tight_layout()
        plt.xlabel('day number')
        plt.ylabel('power (W)')
        savetopath = '../data/plots_sunpower/%s.png' %pltname
        plt.savefig(savetopath, bbox_inches="tight")
        #plt.gcf().subplots_adjust(bottom=0.35)
    #return plt
