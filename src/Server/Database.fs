/// Functions for managing the database.
module ServerCode.Database

open ServerCode

[<RequireQualifiedAccess>]
type DatabaseType =
  | FileSystem

type IDatabaseFunctions =
  abstract member LoadWishList : string -> Async<Domain.WishList>
  abstract member SaveWishList : Domain.WishList -> Async<unit>
  abstract member GetLastResetTime : unit -> Async<System.DateTime>

/// Start the web server and connect to database
let getDatabase databaseType startupTime =
  match databaseType with
  | DatabaseType.FileSystem ->
    { new IDatabaseFunctions with
        member __.LoadWishList key = async { return Storage.FileSystem.getWishListFromDB key }
        member __.SaveWishList wishList = async { return Storage.FileSystem.saveWishListToDB wishList }
        member __.GetLastResetTime () = async { return startupTime } }