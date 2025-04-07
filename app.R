library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)

# Define sensor locations by sensor id with location names.
sensor_locations <- list(
  "2021" = list(name = "Location 1", lat = 49.141444, lng = -123.10827),
  "2022" = list(name = "Location 2", lat = 49.141445, lng = -123.10822),
  "2023" = list(name = "Location 3", lat = 49.14143, lng = -123.10821),
  "2024" = list(name = "Location 4", lat = 49.141448, lng = -123.10826),
  "2026" = list(name = "Location 5", lat = 49.1414410, lng = -123.10823),
  "2030" = list(name = "Location 6", lat = 49.141449, lng = -123.10829),
  "2031" = list(name = "Location 7", lat = 49.141443, lng = -123.108211),
  "2032" = list(name = "Location 8", lat = 49.141442, lng = -123.10822),
  "2033" = list(name = "Location 9", lat = 49.141441, lng = -123.10828),
  "2034" = list(name = "Location 10", lat = 49.141446, lng = -123.10824),
  "2039" = list(name = "Location 11", lat = 49.141444, lng = -123.10822),
  "2040" = list(name = "Location 12", lat = 49.141443, lng = -123.10828),
  "2041" = list(name = "Location 13", lat = 49.141448, lng = -123.10827),
  "2042" = list(name = "Location 14", lat = 49.141446, lng = -123.10829),
  "2043" = list(name = "Location 15", lat = 49.141425, lng = -123.10825),
  "MOD-00616" = list(name = "Location 16", lat = 49.141425, lng = -123.10825),
  "MOD-00632" = list(name = "Location 17", lat = 49.141425, lng = -123.10825), 
  "MOD-00625" = list(name = "Location 18", lat = 49.141425, lng = -123.10825), 
  "MOD-00631" = list(name = "Location 19", lat = 49.141425, lng = -123.10825),
  "MOD-00623" = list(name = "Location 20", lat = 49.141425, lng = -123.10825),
  "MOD-00628" = list(name = "Location 21", lat = 49.141425, lng = -123.10825),
  "MOD-00620" = list(name = "Location 22", lat = 49.141425, lng = -123.10825), 
  "MOD-00627" = list(name = "Location 23", lat = 49.141425, lng = -123.10825), 
  "MOD-00630" = list(name = "Location 24", lat = 49.141425, lng = -123.10825), 
  "MOD-00624" = list(name = "Location 25", lat = 49.141425, lng = -123.10825)
)

# Helper function to get sensor name from sensor id.
getSensorName <- function(sensor_id) {
  if (!is.null(sensor_locations[[sensor_id]]) && !is.null(sensor_locations[[sensor_id]]$name))
    return(sensor_locations[[sensor_id]]$name)
  else return(sensor_id)
}

# Function to choose marker color based on AQHI
getAQHIColor <- function(aqhi) {
  aqhi <- as.numeric(aqhi)
  if (is.na(aqhi)) return("gray")
  if (aqhi <= 1) {
    return("#67c1f1")
  } else if (aqhi <= 2) {
    return("#4e95c7")
  } else if (aqhi <= 3) {
    return("#396798")
  } else if (aqhi <= 4) {
    return("#e7eb38")
  } else if (aqhi <= 5) {
    return("#f1cb2e")
  } else if (aqhi <= 6) {
    return("#e79647")
  } else if (aqhi <= 7) {
    return("#dd6869")
  } else if (aqhi <= 8) {
    return("#d82732")
  } else if (aqhi <= 9) {
    return("#bf2733")
  } else if (aqhi <= 10) {
    return("#8b2328")
  } else {
    return("#5a161b")
  }
}


# Function to return a qualitative description for AQHI.
getAQHIDescription <- function(aqhi) {
  aqhi <- as.numeric(aqhi)
  if (is.na(aqhi)) return("No data available")
  if (aqhi <= 3) {
    return("Low health risk")
  } else if (aqhi <= 6) {
    return("Moderate health risk")
  } else if (aqhi <= 10) {
    return("High health risk")
  } else {
    return("Very high health risk")
  }
}


loadCalibratedData <- function(sensor_ids) {
  data_list <- lapply(sensor_ids, function(sensor_id) {
    pattern <- paste0("^", sensor_id, "_calibrated_.*\\.csv$")
    files <- list.files("calibrated_data", pattern = pattern, full.names = TRUE)
    if (length(files) == 0) return(NULL)
    dates <- sapply(files, function(f) {
      parts <- unlist(strsplit(basename(f), "_"))
      as.Date(parts[3], format = "%Y-%m-%d")
    })
    latest_file <- files[which.max(dates)]
    
    # Read the CSV file.
    df <- read_csv(latest_file, show_col_types = FALSE)
    
    # --- Debugging Code ---
    cat("DEBUG: Processing sensor", sensor_id, "from file:", latest_file, "\n")
    print(sapply(df, class))
    # ----------------------
    
    numeric_cols <- c("AQHI", "CO", "NO", "NO2", "O3", "CO2", "PM1.0", "PM2.5", "PM10",
                      "TE", "T", "RH", "WD", "WS", "PWR", "BATT", "CHRG", "RUN",
                      "SD", "RAW")
    for (col in numeric_cols) {
      if (col %in% names(df)) {
        original <- df[[col]]
        df[[col]] <- as.numeric(df[[col]])
        if (any(is.na(df[[col]]) & !is.na(original))) {
          warning(paste("Conversion to numeric resulted in NA for column", col, "in sensor", sensor_id))
        }
      }
    }
    for (col in names(df)) {
      if (grepl("^-?[0-9.]+$", col)) {
        original <- df[[col]]
        df[[col]] <- as.numeric(df[[col]])
        if (any(is.na(df[[col]]) & !is.na(original))) {
          warning(paste("Conversion to numeric resulted in NA for column", col, "in sensor", sensor_id))
        }
      }
    }
    
    if (nrow(df) == 0) return(NULL)
    latest <- df %>% arrange(desc(DATE)) %>% slice(1)
    latest$sensor_id <- sensor_id
    latest
  })
  bind_rows(data_list)
}

loadHistoricalData <- function(sensor_id) {
  pattern <- paste0("^", sensor_id, "_calibrated_.*\\.csv$")
  files <- list.files("calibrated_data", pattern = pattern, full.names = TRUE)
  if (length(files) == 0) return(NULL)
  dates <- sapply(files, function(f) {
    parts <- unlist(strsplit(basename(f), "_"))
    as.Date(parts[3], format = "%Y-%m-%d")
  })
  latest_file <- files[which.max(dates)]
  df <- read_csv(latest_file, show_col_types = FALSE)
  df <- df %>% mutate(DATE = as.POSIXct(DATE))
  df %>% filter(DATE >= Sys.time() - 24*3600)
}

pollutants <- list(
  "CO" = "ppm",
  "NO" = "ppb",
  "NO2" = "ppb",
  "O3" = "ppb",
  "CO2" = "ppm",
  "PM1.0" = "µg/m³",
  "PM2.5" = "µg/m³",
  "PM10" = "µg/m³"
)

# Create a named vector for dropdown choices using sensor id as the value and sensor name as the label.
sensor_choices <- setNames(names(sensor_locations), sapply(sensor_locations, function(x) x$name))

ui <- fluidPage(
  includeCSS("www/styles.css"),
  tags$head(tags$meta(charset = "utf-8")),
  div(class = "title-bar",
      div(class = "title-left", "iREACH Laboratory"),
      div(class = "title-center", "Community Cleaner Air Spaces"),
      div(class = "title-right",
          img(src = "image1.png", height = "50px"),
          img(src = "image2.png", height = "50px")
      )
  ),
  navbarPage(
    id = "navbar",
    "Air Quality Dashboard",
    tabPanel("Home",
             fluidPage(
               fluidRow(
                 column(12,
                        p("This dashboard displays nothing of interest at this point in time."),
                        div(id = "advisories", style = "background-color: #f8d7da; padding: 10px; border-radius: 5px;",
                            strong("Active Air Quality Advisories:"),
                            p("No advisories are currently active.")
                        ),
                        br(),
                        div(class = "button-container",
                            actionButton("map_page", "View Map", class = "nav-button", style = "background-image: url('map.png');"),
                            actionButton("list_page", "Detailed View", class = "nav-button", style = "background-image: url('list_image.png');"),
                            actionButton("info_page", "Info", class = "nav-button", style = "background-image: url('placeholder-image.jpg');")
                        )
                 )
               )
             )
    ),
    tabPanel("Map",
             fluidPage(
               fluidRow(
                 column(9, leafletOutput("airQualityMap", height = "85vh")),
                 column(3,
                        selectInput("sensor_select", "Select Sensor", choices = sensor_choices, selected = ""),
                        uiOutput("sensor_details"),
                        textOutput("last_update")
                 )
               )
             )
    ),
    tabPanel("Detailed View",
             fluidPage(
               selectInput("list_sensor_select", "Select Sensor", choices = sensor_choices, selected = ""),
               uiOutput("sensor_info")
             )
    ),
    tabPanel("Info",
             fluidPage(
               h2("Information Page - Coming Soon")
             )
    )
  )
)

server <- function(input, output, session) {
  
  # Navigation between tabs.
  observeEvent(input$map_page, { updateNavbarPage(session, "navbar", selected = "Map") })
  observeEvent(input$list_page, { updateNavbarPage(session, "navbar", selected = "Detailed View") })
  observeEvent(input$info_page, { updateNavbarPage(session, "navbar", selected = "Info") })
  
  sensor_data <- reactive({ loadCalibratedData(names(sensor_locations)) })
  
  output$airQualityMap <- renderLeaflet({
    m <- leaflet() %>% addTiles()
    df <- sensor_data()
    if(nrow(df) > 0) {
      df <- df %>%
        mutate(
          marker_color = sapply(AQHI, getAQHIColor),
          popup_text = paste0("<b>", sapply(sensor_id, getSensorName), " (", sensor_id, ")</b><br>",
                              "AQHI: ", round(as.numeric(AQHI), 1), "<br>")
        )
      for(i in 1:nrow(df)) {
        sensor_id <- df$sensor_id[i]
        loc <- sensor_locations[[sensor_id]]
        if(!is.null(loc)) {
          m <- m %>% addCircleMarkers(
            lng = loc$lng, lat = loc$lat,
            color = df$marker_color[i], radius = 8, fillOpacity = 0.8,
            popup = df$popup_text[i], layerId = sensor_id
          )
        }
      }
    }
    m
  })
  
  
  observeEvent(input$sensor_select, {
    sensor_id <- input$sensor_select
    if (sensor_id != "" && sensor_id %in% names(sensor_locations)) {
      loc <- sensor_locations[[sensor_id]]
      leafletProxy("airQualityMap") %>% setView(lng = loc$lng, lat = loc$lat, zoom = 12)
    }
  })
  
  output$last_update <- renderText({
    df <- sensor_data()
    if(nrow(df) == 0) {
      "No data available"
    } else {
      last_update <- max(as.POSIXct(df$DATE), na.rm = TRUE)
      last_update <- force_tz(last_update, tzone = "UTC")
      last_update_pst <- last_update - lubridate::hours(7)
      paste("Last updated:", format(last_update_pst, "%Y-%m-%d %H:%M"), "PST")
    }
  })
  
  output$sensor_details <- renderUI({
    selected_sensor <- if(!is.null(input$sensor_select) && input$sensor_select != "") {
      input$sensor_select
    } else if(!is.null(input$airQualityMap_marker_click)) {
      input$airQualityMap_marker_click$id
    } else { 
      NULL 
    }
    if(is.null(selected_sensor)) return(NULL)
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == selected_sensor)
    if(nrow(sensor_row) == 0) return(NULL)
    wellPanel(
      h4(paste("Sensor", selected_sensor, "(", getSensorName(selected_sensor), ") Details")),
      tags$table(class = "data-table",
                 tags$thead(
                   tags$tr(
                     tags$th("Pollutant"),
                     tags$th("Value"),
                     tags$th("Unit")
                   )
                 ),
                 tags$tbody(
                   tags$tr(
                     tags$td("CO"),
                     tags$td(round(as.numeric(sensor_row$CO)[1], 1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("NO"),
                     tags$td(round(as.numeric(sensor_row$NO)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("NO2"),
                     tags$td(round(as.numeric(sensor_row$NO2)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("O3"),
                     tags$td(round(as.numeric(sensor_row$O3)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("CO2"),
                     tags$td(round(as.numeric(sensor_row$CO2)[1], 1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("PM1.0"),
                     tags$td(round(as.numeric(sensor_row$`PM1.0`)[1], 1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM2.5"),
                     tags$td(round(as.numeric(sensor_row$`PM2.5`)[1], 1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM10"),
                     tags$td(round(as.numeric(sensor_row$PM10)[1], 1)),
                     tags$td("µg/m³")
                   )
                 )
      )
    )
  })
  
  output$sensor_info <- renderUI({
    req(input$list_sensor_select)
    sensor_id <- input$list_sensor_select
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == sensor_id)
    if(nrow(sensor_row) == 0) return("No sensor data available.")
    description <- getAQHIDescription(sensor_row$AQHI[1])
    pollutant_ui <- lapply(names(pollutants), function(poll) {
      value <- round(as.numeric(sensor_row[[poll]][1]), 1)
      div(class = "pollutant-item", paste0(poll, ": ", value, " ", pollutants[[poll]]))
    })
    tagList(
      div(class = "sensor-info-box",
          h4(paste("Air Quality for", getSensorName(sensor_id), "(", sensor_id, ")")),
          p(description),
          tags$details(
            tags$summary("More info"),
            div(
              tagList(pollutant_ui),
              tags$details(
                tags$summary("Show 24-hour AQHI graph"),
                plotOutput("selected_sensor_plot")
              )
            )
          )
      )
    )
  })
  
  output$selected_sensor_plot <- renderPlot({
    req(input$list_sensor_select)
    sensor_id <- input$list_sensor_select
    hist_data <- loadHistoricalData(sensor_id)
    req(nrow(hist_data) > 0)
    hist_data <- hist_data %>% mutate(DATE = as.POSIXct(DATE))
    aqhi_values <- as.numeric(hist_data$AQHI)
    plot(hist_data$DATE, aqhi_values, type = "l", lwd = 2,
         xlab = "Time", ylab = "AQHI",
         main = paste("24-hour AQHI for", getSensorName(sensor_id), "(", sensor_id, ")"))
  })
}

shinyApp(ui, server)
