exetera <- reticulate::import('exetera')
exeteracovid <- reticulate::import('exeteracovid')
builtins <- reticulate::import_builtins()

Session <- function(){
  s <- exetera$core$session$Session()
  return (s)
}
