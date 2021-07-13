df.keys <- function(df){
  l = builtins$list(df$keys())
  return (l)
}

df.copy <- function(field, df, name){
  exetera$core$dataframe$copy(field, df, name)
  return (df[name])
}

df.rm <- function(df, name){  # remove/delete a field from dataframe by field name (str)
  df$delete_field(df[name])
}

df.write_csv <- function(df_s, fname){
  df_s_name = df.keys(df_s)
  #length check
  for (i in seq(2, length(df_s_name))){
   if(fld.length(df_s[df_s_name[i]]) != fld.length(df_s[df_s_name[1]])){
     print("Dataframe length is not same among all columns.")
     return (NULL)
   }
  }
  #compose data.frame
  for (i in seq(length(df_s_name))){
    if(i==1){
      out_df = data.frame(fld.data(df_s[df_s_name[i]]))
    }else{
      out_df = cbind(out_df, fld.data(df_s[df_s_name[i]]))
    }
  }
  #name and write
  colnames(out_df) = df_s_name
  write.csv2(out_df, fname)
}
