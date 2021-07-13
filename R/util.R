
util.uniq_c <- function(data){
  tb = table(unlist(data))
  rslt = c()
  rslt[[1]] = names(tb)
  rslt[[2]] = as.vector(tb)
  return (rslt)
}
