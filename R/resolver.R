#appease devtools check()
globalVariables(c("join2", "distance", ".", "join"))

###resolve database join concepts
resolver <- function(databases, joiners, helpers = NULL, cache = TRUE, cache_loc = getwd(), joiners2 = NULL) {

  #FIXME: sloppy cache step -> tighten up

  #wd files
  files <- list.files(cache_loc)

  #detect existing cache
  if(any(stringr::str_detect(files, "resolve_cache.rds"))) {

    use_cache <- readline(prompt = "Cache detected. Use? Ensure cache matches current task! (y/n): ")

    if(stringr::str_to_lower(use_cache) == "y") {

      product <- readRDS("resolve_cache.rds")

      return(product)

    }
  }

  #column name errors
  if(any(purrr::map_lgl(databases, ~any(stringr::str_detect(names(.x), "join|\\.x|\\.y")))) == TRUE) {

    stop("Error: Column names include 'join', '.x', or '.y'! These are reserved keywords. Rename columns.")

  }

  #NA joiners error
  if(any(is.na(databases[[1]][[joiners[[1]]]])) == TRUE) {

    #bc it makes no sense to have NAs in the reference join column...
    stop("Error: NAs detected in database 1's join column! Fix!")

  }

  #list error
  if(!(inherits(databases, "list")) | !(inherits(joiners, "list"))) {
    stop("Error: must input lists")
  }

  if(length(databases) != length(joiners)) {
    stop("Error: databases and joiners are unequal lengths")
  }

  #joiner error
  if(length(unique(joiners) < length(joiners))) {
    warning("Warning: identical joiners, renaming...")

    new_names <- list()

    for(i in 1:length(joiners)) {

      new_names[[i]] <- paste0(joiners[[i]], i)

      databases[[i]] <- dplyr::rename_with(databases[[i]], ~paste0(new_names[[i]]), .cols = joiners[[i]])

    }

    joiners <- new_names

  }

  #joiner2 error
  if(inherits(joiners2, "list")) {

    if(length(databases) != length(joiners2)) {

      stop("Error: databases and joiners2 are unequal length")

    }

    if(length(unique(joiners2)) < length(joiners2)) {

      warning("Warning: identical joiners2, renaming...")

      new_names <- list()

      for(i in 1:length(joiners2)) {

        new_names[[i]] <- paste0(joiners2[[i]], i)

        databases[[i]] <- dplyr::rename_with(databases[[i]], ~paste0(new_names[[i]]), .cols = joiners2[[i]])

      }

      joiners2 <- new_names

    }

  }

  use_helpers <- FALSE

  #helpers error
  if(inherits(helpers, "list")) {

    if(length(helpers) != length(databases)) {

      warning("Warning: helpers length =/= databases length\nContinuing without helpers")

      use_helpers <- FALSE

    }

    use_helpers <- TRUE

  }

  #length
  length <- length(databases)

  #set join columns
  for(i in 1:length) {

    join_column <- databases[[i]][[joiners[[i]]]]

    databases[[i]] <- dplyr::bind_cols(databases[[i]], join = join_column)

    #set join2 columns
    if(inherits(joiners2, "list")) {

      join2_column <- databases[[i]][[joiners2[[i]]]]

      databases[[i]] <- dplyr::bind_cols(databases[[i]], join2 = join2_column)

    }

  }

  #prepare join fails list
  join_fails <- list()

  #iterative join
  joined_databases <- databases[[1]]

  for(i in 1:(length - 1)) {

    joined_databases <- dplyr::left_join(joined_databases, databases[[i + 1]], by = "join", keep = NULL)

    #detect join failure
    fail_ind <- which(is.na(joined_databases[[joiners[[i + 1]]]]))

    #where fail
    fails <- joined_databases[[joiners[[1]]]][fail_ind]

    #print fails
    cat(paste0("Join", " fails (db ",  "1" , " -> ", i+1, "): ", do.call(paste, as.list(paste0(fails, " "))), "\n"))

    #save to join fails
    join_fails[[i]] <- fails

  }

  #name join fails
  names(join_fails) <- paste0("join_", 1:(length-1))

  #flatten join failures
  unique_fails <- unique(purrr::list_c(join_fails))

  #joiners2 resolve
  if(inherits(joiners2, "list")) {

    cat("\nJoiners2 detected. Attempting to resolve conflicts.\n")

    Sys.sleep(1)

    joined_databases <- databases[[1]]

    #filter for issue rows
    joined_databases <- joined_databases[joined_databases[[joiners[[1]]]] %in% unique_fails,]

    #drop join2 NAs
    na_fails <- dplyr::filter(joined_databases, is.na(join2))
    na_fails <- na_fails[[joiners[[1]]]]

    #filter NAs
    joined_databases <- dplyr::filter(joined_databases, !is.na(join2))

    #prepare join2 fails list
    join2_fails <- c()

    for(i in 1:(length - 1)) {

      #iterative join
      joined_databases <- dplyr::left_join(joined_databases, databases[[i + 1]], by = "join2", keep = NULL)

      #detect join failure
      fail_ind <- which(is.na(joined_databases[[joiners2[[i + 1]]]]))

      #where fail
      fails <- joined_databases[[joiners[[1]]]][fail_ind]

      #print fails
      #printing these updates makes the function more confusing for users!
      #cat(paste0("Join ",  i , " fails: ", do.call(paste, as.list(paste0(fails, " "))), "\n"))

      #join2_fails
      join2_fails <- c(join2_fails, fails)

    }

    #add na_fails
    join2_fails <- unique(c(join2_fails, na_fails))

    #filter join_fails with join2_fails
    for(i in 1:length(join_fails)) {

      join_fails[[i]] <- join_fails[[i]][join_fails[[i]] %in% join2_fails]

    }

    #successful joins
    unique_success <- unique_fails[!(unique_fails %in% join2_fails)]

    Sys.sleep(1)

    #print resolves
    if(length(unique_success) > 0) {

      cat(paste0("Resolved: ", do.call(paste, as.list(paste0(unique_success, " ")))))

    } else {

      cat("Nothing resolved!")

    }

    Sys.sleep(1)

    #update unique fails
    unique_fails <- join2_fails

  }

  #pass resolve query if perfect join
  if(length(unique_fails) == 0) {

    cat("\nAll conflicts resolved!\n")

    Sys.sleep(2)

    resolve <- "y"

  } else {

    #ask for resolve
    resolve <- readline(prompt = "Resolve remaining join failures? (y/n)")

  }

  #branch
  if(stringr::str_to_lower(resolve) != "y") {

    cat("Finished.")

    return(join_fails)


  } else {

    #create empty crosswalk
    crosswalk <- matrix(nrow = length(unique_fails), ncol = length)
    crosswalk[,1] <- unique_fails

    #set pass_token
    pass_token <-  FALSE

    #resolve conflicts
    for(i in 1:(length-1)) {

      #skip empty conflicts
      if(length(join_fails[[i]]) == 0) {next}

      for(j in 1:length(join_fails[[i]])) {

        while(TRUE) {

          #parse helpers
          if(use_helpers == TRUE) {

            #fail helper
            row_loc <- which(databases[[1]][[joiners[[1]]]] == join_fails[[i]][j])
            fail_helper <- databases[[1]][[row_loc, helpers[[1]]]]

            #candidate helpers
            cand_helper <- databases[[i + 1]][[helpers[[i + 1]]]]

          }

          #print conflict
          cat(paste0("Resolving: ", join_fails[[i]][j], " (db ", "1", " -> ", i+1, ")  "))

          #print helper
          if(use_helpers == TRUE) {

            cat(paste0("Helper: ", fail_helper, "  "))

          }

          #get candidates
          cands <- databases[[i + 1]][[joiners[[i + 1]]]]

          #calculate stringdist
          dists <- stringdist::stringdist(join_fails[[i]][j], cands, method = "qgram")

          #bind
          cands <- dplyr::bind_cols(candidate = cands, distance = dists)

          #use_helpers
          if(use_helpers == TRUE) {

            cands <- cands %>%
              dplyr::bind_cols(helper = cand_helper)

          }

          #arrange and rank
          cands <- cands %>%
            dplyr::arrange(distance) %>%
            dplyr::bind_cols(., rank = 1:nrow(.))

          #set slice indices
          bottom_slice <- 1
          upper_slice <- 20

          #filter top 10
          top <- dplyr::slice(cands, bottom_slice:upper_slice)

          #display candidates
          print(top[,-2])

          #make selection
          selection <- readline(prompt = paste0("Select synonym by rank ", "(", bottom_slice, "-", upper_slice, "/expand/search/manual/pass)\n"))

          #expand mode
          while(stringr::str_to_lower(selection) == "expand") {

            bottom_slice <- bottom_slice + 20
            upper_slice <- upper_slice + 20

            top <- dplyr::slice(cands, bottom_slice:upper_slice)

            #print conflict
            cat(paste0("Resolving: ", join_fails[[i]][j], " (db ", "1", " -> ", i+1, ")  "))

            #print helper
            if(use_helpers == TRUE) {

              cat(paste0("Helper: ", fail_helper, "  "))

            }

            print(top[,-2])

            selection <- readline(prompt = paste0("Select synonym by rank ", "(", bottom_slice, "-", upper_slice, "/expand/search/manual/pass)\n"))

          }

          #manual mode
          if(stringr::str_to_lower(selection) == "manual") {

            selection <- readline(prompt = paste0("Input synonym for ", join_fails[[i]][[j]], " in database ", i + 1, " (or back): "))

            #check if manual entry is valid
            if(stringr::str_to_lower(selection) %in% databases[[i+1]][joiners[[i+1]]] == FALSE) {

              cat("Manual entry not contained in db", i + 1, "\nTry again!", "\n")

              Sys.sleep(2)

              next

            }

            crosswalk[crosswalk[,1] == join_fails[[i]][[j]], i + 1] <- selection

            if(stringr::str_to_lower(selection) != "back") {

              break

            }

          }

          #pass mode
          if(stringr::str_to_lower(selection) == "pass") {

            crosswalk[crosswalk[,1] == join_fails[[i]][[j]], i + 1] <- "*"

            pass_token <- TRUE

            break

          }

          #search mode
          if(stringr::str_to_lower(selection) == "search") {

            searcher <- readline(prompt = "Input search:")

            cat(paste0("Searching: ", searcher))

            if(use_helpers == TRUE) {

              search_helpers <- cands[["helper"]]

            }

            cands <- cands[["candidate"]]

            dists <- stringdist::stringdist(searcher, cands, method = "qgram")

            cands <- dplyr::bind_cols(candidate = cands, distance = dists)

            if(use_helpers == TRUE) {

              cands <- cands %>%
                dplyr::bind_cols(helper = search_helpers)

            }

            cands <- cands %>%
              dplyr::arrange(distance) %>%
              dplyr::bind_cols(., rank = 1:nrow(.))

            bottom_slice <- 1
            upper_slice <- 20

            top <- dplyr::slice(cands, bottom_slice:upper_slice)

            print(top[,-2])

            selection <- readline(prompt = paste0("Select synonym by rank ", "(", bottom_slice, "-", upper_slice, "/back)\n"))

          }

          if(stringr::str_to_lower(selection) != "back") {

            #check for valid response
            if(!is.na(as.numeric(selection))) {

              #convert to numeric
              selection <- as.numeric(selection)

              #save to crosswalk
              crosswalk[crosswalk[,1] == join_fails[[i]][[j]], i + 1] <- cands[[selection,1]]

              #break while loop
              break

            } else {

              cat("Invalid selection. Try again.\n\n")

              Sys.sleep(2)

            }

          }

        }

      }

    }

    #joiner2 join
    if(inherits(joiners2, "list")) {

      #filter joiners2 rows
      databases_join2 <- databases[[1]] %>%
        dplyr::filter(join %in% unique_success)

      #cut joiners2 rows from databases
      databases[[1]] <- databases[[1]] %>%
        dplyr::filter(!(join %in% unique_success))

      #join using join2
      joined2_databases <- databases_join2

      for(i in 2:length) {

        joined2_databases <- dplyr::left_join(joined2_databases, databases[[i]], by = "join2")

      }

    }

    #updated join column
    for(i in 1:length) {

      databases[[i]] <- dplyr::mutate(databases[[i]], join_update = join)

    }

    for(i in 2:length) {

      if(nrow(crosswalk) > 1) {

        #condense crosswalk
        cross <- crosswalk[,c(1,i)][!is.na(crosswalk[,i]),]

      } else {

        cross <- crosswalk[c(1,i)]

      }

      #ensure cross is a matrix
      if(is.matrix(cross) == FALSE) {

        cross <- matrix(cross, nrow = length(cross) / 2, ncol = 2, byrow = TRUE)

      }

      #replace join_update values
      for(j in 1:nrow(cross)) {

        conflict <- cross[[j,2]]

        conflict_loc <- which(databases[[i]][["join_update"]] == conflict)

        databases[[i]][["join_update"]][conflict_loc] <- cross[[j,1]]

      }

    }

    #join on join_update
    product <- databases[[1]]

    for(i in 2:length) {

      product <- dplyr::left_join(product, databases[[i]], by = "join_update")

    }

    #bind joiner2 rows
    if(inherits(joiners2, "list")) {

      product <- dplyr::bind_rows(product, joined2_databases)


    }

    #rm join columns
    product <- product[,stringr::str_detect(names(product), "join", negate = TRUE)]

    #rm extra columns
    product <- product[,stringr::str_detect(names(product), "\\.x|\\.y", negate = TRUE)]

  }

  #return final and crosswalk
  if(pass_token == TRUE) {

    warning("Warning: by using pass option resulting join is invalid")

  }

  if(cache == TRUE) {

    saveRDS(list(product, crosswalk), file = "resolve_cache.rds")

  }

  return(list(product, crosswalk))

} #fxn end

#vignette
if(sys.nframe() == 0) {

  #library
  library(readxl)
  library(dplyr)
  library(taxadb)

  #import eg data
  taxonomy <- read.csv("data/taxonomy.csv")
  avonet <- read.csv("data/avonet.csv")
  eltontraits <- read.csv("data/eltontraits.csv")
  nest_heights <- read.csv("data/nest_heights.csv")

  #add itis ids
  taxonomy <- taxonomy %>% mutate(id = get_ids(scientific_name))
  avonet <- avonet %>% mutate(id = get_ids(Species2))
  eltontraits <- eltontraits %>% mutate(id = get_ids(Scientific))
  nest_heights <- nest_heights %>% mutate(id = get_ids(scientific_name))

  #test fxn
  resolver(
    databases = list(taxonomy, avonet, eltontraits, nest_heights),
    joiners = list("common_name", "common_name", "English", "common_name"),
    helpers = list("scientific_name","Species2","Scientific","scientific_name"),
    joiners2 = list("id", "id", "id", "id")
  )

}
