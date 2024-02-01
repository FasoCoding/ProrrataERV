from termgraph import termgraph as tg
from colorama import init


def graph_results():
    init()
    args_default = tg.init_args()
    args_default["title"] = "Resultados prorrata ERV"
    args_default["color"] = ["blue,red"]
    args_default["filename"] = "results.csv"

    _, labels, data, colors = tg.read_data(args_default)
    catergories = ["Generación","Prorrata"]
    colors = ["blue","red"]
    tg.print_categories(catergories, colors)
    tg.chart(colors, data, args_default, labels)
