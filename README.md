# azure_databrick_class
A tool class that I wrote for working with notebook in backend
  - Most people will run databrick via data factory, but it is also possible to run / schedule a run via databrick resful API
  - Somehow MS api doesn't recursively list a directory as Blob Storage does, so I just add that functionality in "ws_list()".
