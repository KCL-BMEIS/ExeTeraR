fld.data <- function(field, index=NULL){
  if (length(index)==0){
    all = builtins$slice(NULL)
    return (field$data[all])
  }else{
    return (field$data[index])
  }
}

fld.length <- function(field){
  return ( field$data$'__len__'())
}






