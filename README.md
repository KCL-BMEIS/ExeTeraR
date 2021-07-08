# ExeTeraR
By using the ExeTera R Wrapper, you can call ExeTera functions from R environment.

For more details, please refer to https://github.com/KCL-BMEIS/ExeTera/wiki/Using-ExeTera-With-R

# ExeTera
ExeTera is a reproducible analysis pipelines for large tabular datasets. https://github.com/KCL-BMEIS/ExeTera

By utlizing column based data storage and efficient data processing algorithms, ExeTera can handle 100G+ of dataset on a single machine. Benefit from the community powered Python libraries, ExeTera provides easiler access and better performance than SQL


# Quick Guide
Please follow the steps below to use ExeTera R wrapper:

1, Make sure you have installed python, ExeTera and ExeTeraCovid;

2, Make sure you have installed R environment, and two R packages: reticulate(https://rstudio.github.io/reticulate/) and devtools.

3, You can then load the the wrapper through load_all('/path/to/the/wrapper/code')

4, For run the example notebook in Jupyter, please install the R package IRkernel(https://github.com/IRkernel/IRkernel)
