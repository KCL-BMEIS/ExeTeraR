ds.keys <- function(ds){
  l = builtins$list(ds$keys())
  return (l)
}

ds.rm <- function(ds, df){  # remove a dataframe from ds
 ds$delete_dataframe(df)
}
