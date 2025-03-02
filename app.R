library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)

# Define sensor locations by sensor id (update as needed)
sensor_locations <- list(
  "2035" = list(lat = 49.2827, lng = -123.1207),
  "2029" = list(lat = 49.4, lng = -123.7)
  # Add additional sensors here
)

# Function to choose marker color based on AQI
getAQIColor <- function(aqi) {
  aqi <- as.numeric(aqi)
  if (is.na(aqi)) return("gray")
  if (aqi <= 50) {
    "green"
  } else if (aqi <= 100) {
    "yellow"
  } else if (aqi <= 150) {
    "orange"
  } else if (aqi <= 200) {
    "red"
  } else if (aqi <= 300) {
    "purple"
  } else {
    "maroon"
  }
}

# Load calibrated data (latest row) for each sensor.
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
    df <- read_csv(latest_file, show_col_types = FALSE)
    if (nrow(df) == 0) return(NULL)
    latest <- df %>% arrange(desc(DATE)) %>% slice(1)
    latest$sensor_id <- sensor_id
    latest
  })
  bind_rows(data_list)
}

# Load historical data (last 24 hours) for a sensor.
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

# Define pollutants (CSV column names) and their units.
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

# Use reactiveValues to track the sensor and pollutant selected in the List tab.
rv <- reactiveValues(sensor = NULL, pollutant = NULL)

ui <- fluidPage(
  tags$head(tags$style(HTML("
    body { margin: 0; padding: 0; }
    .title-bar {
      width: 100vw; background-color: #002145; color: white; padding: 20px;
      display: flex; justify-content: space-between; align-items: center; box-sizing: border-box;
    }
    .title-left { font-size: 20px; font-weight: bold; }
    .title-center { font-size: 24px; font-weight: bold; text-align: center; flex-grow: 1; }
    .title-right { display: flex; flex-direction: column; align-items: center; }
    .nav-button {
      width: 30vw; height: 30vw; max-width: 300px; max-height: 300px;
      background-size: cover; background-position: center; border: none;
      cursor: pointer; font-size: 24px; text-align: center; color: white;
      font-weight: bold; opacity: 0.8; display: flex; align-items: center; justify-content: center;
      text-shadow: 2px 2px 4px black;
    }
    .nav-button:hover { opacity: 1; text-shadow: none; }
    .button-container { display: flex; justify-content: space-around; margin-top: 20px; }
    details { margin-bottom: 15px; background: #f7f7f7; padding: 10px; border-radius: 5px; }
    summary { font-size: 16px; font-weight: bold; cursor: pointer; }
    /* Neat table styling */
    .data-table {
      width: 100%; border-collapse: collapse; margin-top: 10px;
    }
    .data-table th, .data-table td {
      border: 1px solid #ddd; padding: 8px;
    }
    .data-table tr:nth-child(even) { background-color: #f9f9f9; }
    .data-table tr:hover { background-color: #f1f1f1; }
    .data-table th { background-color: #002145; color: white; }
    /* Inline plot styling */
    .inline-plot { margin-top: 10px; }
  "))),
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
                            actionButton("list_page", "View List", class = "nav-button", style = "background-image: url('placeholder-image.jpg');"),
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
                        selectInput("sensor_select", "Select Sensor", choices = names(sensor_locations), selected = ""),
                        uiOutput("sensor_details"),
                        textOutput("last_update")
                 )
               )
             )
    ),
    tabPanel("List",
             fluidPage(
               uiOutput("sensor_list")
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
  observeEvent(input$list_page, { updateNavbarPage(session, "navbar", selected = "List") })
  observeEvent(input$info_page, { updateNavbarPage(session, "navbar", selected = "Info") })
  
  sensor_data <- reactive({ loadCalibratedData(names(sensor_locations)) })
  
  output$airQualityMap <- renderLeaflet({
    m <- leaflet() %>% addTiles()
    df <- sensor_data()
    if(nrow(df) > 0) {
      df <- df %>%
        mutate(
          marker_color = sapply(AQI, getAQIColor),
          popup_text = paste0("<b>Sensor: ", sensor_id, "</b><br>",
                              "AQI: ", round(as.numeric(AQI), 1), "<br>")
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
    if(nrow(df)==0) {
      "No data available"
    } else {
      last_update <- max(as.POSIXct(df$DATE), na.rm = TRUE)
      last_update <- force_tz(last_update, tzone="UTC")
      last_update_pst <- last_update - lubridate::hours(16)
      paste("Last updated:", format(last_update_pst, "%Y-%m-%d %H:%M"), "PST")
    }
  })
  
  output$sensor_details <- renderUI({
    selected_sensor <- if(!is.null(input$sensor_select) && input$sensor_select!="") {
      input$sensor_select
    } else if(!is.null(input$airQualityMap_marker_click)) {
      input$airQualityMap_marker_click$id
    } else { NULL }
    if(is.null(selected_sensor)) return(NULL)
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == selected_sensor)
    if(nrow(sensor_row)==0) return(NULL)
    wellPanel(
      h4(paste("Sensor", selected_sensor, "Details")),
      tags$table(class="data-table",
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
                     tags$td(round(as.numeric(sensor_row$CO),1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("NO"),
                     tags$td(round(as.numeric(sensor_row$NO),1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("NO2"),
                     tags$td(round(as.numeric(sensor_row$NO2),1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("O3"),
                     tags$td(round(as.numeric(sensor_row$O3),1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("CO2"),
                     tags$td(round(as.numeric(sensor_row$CO2),1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("PM1.0"),
                     tags$td(round(as.numeric(sensor_row$`PM1.0`),1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM2.5"),
                     tags$td(round(as.numeric(sensor_row$`PM2.5`),1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM10"),
                     tags$td(round(as.numeric(sensor_row$PM10),1)),
                     tags$td("µg/m³")
                   )
                 )
      )
    )
  })
  
  # Build the sensor list in the List tab.
  output$sensor_list <- renderUI({
    df <- sensor_data()
    if(nrow(df)==0) return("No sensor data available.")
    
    sensor_panels <- lapply(1:nrow(df), function(i) {
      sensor_id <- df$sensor_id[i]
      summary_text <- paste0("Sensor ", sensor_id, 
                             " - AQI: ", round(as.numeric(df$AQI[i]),1),
                             ", PM2.5: ", round(as.numeric(df$`PM2.5`[i]),1))
      
      # Build pollutant rows as clickable links.
      poll_rows <- lapply(names(pollutants), function(poll) {
        link_id <- paste0("poll_", sensor_id, "_", poll)
        value <- round(as.numeric(df[[poll]][i]), 1)
        unit <- pollutants[[poll]]
        tags$tr(
          tags$td(actionLink(link_id, poll, style="cursor:pointer; color: #007BFF; text-decoration: underline;")),
          tags$td(style="padding: 5px;", value),
          tags$td(style="padding: 5px;", unit)
        )
      })
      
      # Assemble a details panel for the sensor.
      # If this sensor is selected in rv, show an inline plot below the table.
      inlinePlot <- NULL
      if (!is.null(rv$sensor) && rv$sensor == sensor_id && !is.null(rv$pollutant)) {
        inlinePlot <- tags$div(class="inline-plot", plotOutput(paste0("inline_plot_", sensor_id)))
      }
      
      tags$div(
        style="margin-bottom:15px; padding:10px; border:1px solid #ddd; border-radius:5px; background-color:#fff;",
        tags$details(
          tags$summary(summary_text),
          tags$table(class="data-table", do.call(tagList, poll_rows))
        ),
        inlinePlot
      )
    })
    do.call(tagList, sensor_panels)
  })
  
  # Create observers for pollutant action links in the list.
  observe({
    df <- sensor_data()
    if(nrow(df)==0) return()
    for(i in 1:nrow(df)) {
      sensor_id <- df$sensor_id[i]
      for(poll in names(pollutants)) {
        link_id <- paste0("poll_", sensor_id, "_", poll)
        local({
          s_id <- sensor_id
          p_name <- poll
          l_id <- link_id
          observeEvent(input[[l_id]], {
            rv$sensor <- s_id
            rv$pollutant <- p_name
          }, ignoreNULL = TRUE, ignoreInit = TRUE)
        })
      }
    }
  })
  
  # Render inline plot for the selected sensor in the List tab.
  observe({
    req(rv$sensor, rv$pollutant)
    s_id <- rv$sensor
    p_name <- rv$pollutant
    out_id <- paste0("inline_plot_", s_id)
    output[[out_id]] <- renderPlot({
      hist_data <- loadHistoricalData(s_id)
      req(nrow(hist_data) > 0)
      hist_data <- hist_data %>% mutate(DATE = as.POSIXct(DATE))
      y_values <- as.numeric(hist_data[[p_name]])
      y_values <- round(y_values, 1)
      plot(hist_data$DATE, y_values, type="l", lwd=2,
           xlab="Time", ylab=paste(p_name, " (", pollutants[[p_name]], ")", sep=""),
           main=paste("Sensor", s_id, "-", p_name, "over past 24 hours"))
    })
  })
  
  # The modal plot (for Map tab) remains unchanged.
  output$pollution_plot <- renderPlot({
    req(rv$sensor, rv$pollutant)
    hist_data <- loadHistoricalData(rv$sensor)
    req(nrow(hist_data) > 0)
    hist_data <- hist_data %>% mutate(DATE = as.POSIXct(DATE))
    y_values <- as.numeric(hist_data[[rv$pollutant]])
    y_values <- round(y_values, 1)
    plot(hist_data$DATE, y_values, type="l", lwd=2,
         xlab="Time", ylab=paste(rv$pollutant, " (", pollutants[[rv$pollutant]], ")", sep=""),
         main=paste("Sensor", rv$sensor, "-", rv$pollutant, "over past 24 hours"))
  })
}

shinyApp(ui, server)
