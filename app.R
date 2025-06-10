library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)
library(ggplot2)

# ----------------------------------------------------------------------------
#  1.  Sensor location definitions
# ----------------------------------------------------------------------------
sensor_locations <- list(
  "2021"      = list(name="Location 1",  lat=49.141444,      lng=-123.10827),
  "2022"      = list(name="Location 2",  lat=49.141445,      lng=-123.10822),
  "2023"      = list(name="Location 3",  lat=49.141430,      lng=-123.10821),
  "2024"      = list(name="Location 4",  lat=49.141448,      lng=-123.10826),
  "2026"      = list(name="Location 5",  lat=49.1414410,     lng=-123.10823),
  "2030"      = list(name="Location 6",  lat=49.141449,      lng=-123.10829),
  "2031"      = list(name="Location 7",  lat=49.141443,      lng=-123.108211),
  "2032"      = list(name="Location 8",  lat=49.141442,      lng=-123.10822),
  "2033"      = list(name="Location 9",  lat=49.141441,      lng=-123.10828),
  "2034"      = list(name="Location 10", lat=49.141446,      lng=-123.10824),
  "2039"      = list(name="Location 11", lat=49.141444,      lng=-123.10822),
  "2040"      = list(name="Location 12", lat=49.141443,      lng=-123.10828),
  "2041"      = list(name="Location 13", lat=49.141448,      lng=-123.10827),
  "2042"      = list(name="Location 14", lat=49.141446,      lng=-123.10829),
  "2043"      = list(name="Location 15", lat=49.141425,      lng=-123.10825),
  "MOD-00616" = list(name="Location 16", lat=49.141425,      lng=-123.10825),
  "MOD-00632" = list(name="Location 17", lat=49.141425,      lng=-123.10825),
  "MOD-00625" = list(name="Location 18", lat=49.141425,      lng=-123.10825),
  "MOD-00631" = list(name="Location 19", lat=49.141425,      lng=-123.10825),
  "MOD-00623" = list(name="Location 20", lat=49.16117455543103, lng=-122.96617030713607),
  "MOD-00628" = list(name="Location 21", lat=49.141425,      lng=-123.10825),
  "MOD-00620" = list(name="Location 22", lat=49.141425,      lng=-123.10825),
  "MOD-00627" = list(name="Location 23", lat=49.141425,      lng=-123.10825),
  "MOD-00630" = list(name="Location 24", lat=49.141425,      lng=-123.10825),
  "MOD-00624" = list(name="Location 25", lat=49.141425,      lng=-123.10825)
)

getSensorName <- function(id) {
  if (!is.null(sensor_locations[[id]]$name)) sensor_locations[[id]]$name else id
}

# 2. Helpers
parse_datetime <- function(x) {
  x_chr <- as.character(x)
  dt <- suppressWarnings(ymd_hms(x_chr, tz = "America/Vancouver"))
  if (all(is.na(dt))) dt <- suppressWarnings(ymd_hm(x_chr, tz = "America/Vancouver"))
  if (all(is.na(dt))) dt <- suppressWarnings(ymd(x_chr,    tz = "America/Vancouver"))
  as.POSIXct(dt, tz = "America/Vancouver")
}

getAQHIColor <- function(aqhi) {
  a <- as.numeric(aqhi)
  if (is.na(a)) return("gray")
  else if (a <= 1) return("#67c1f1")
  else if (a <= 2) return("#4e95c7")
  else if (a <= 3) return("#396798")
  else if (a <= 4) return("#e7eb38")
  else if (a <= 5) return("#f1cb2e")
  else if (a <= 6) return("#e79647")
  else if (a <= 7) return("#dd6869")
  else if (a <= 8) return("#d82732")
  else if (a <= 9) return("#bf2733")
  else if (a <= 10) return("#8b2328")
  else return("#5a161b")
}

getAQHIDescription <- function(aqhi) {
  a <- as.numeric(aqhi)
  if (is.na(a))      "No data available"
  else if (a <= 3)   "Low health risk"
  else if (a <= 6)   "Moderate health risk"
  else if (a <= 10)  "High health risk"
  else               "Very high health risk"
}

# 3. Data loaders
loadCalibratedData <- function(sensor_ids) {
  rows <- lapply(sensor_ids, function(id) {
    pattern <- paste0("^", id, "_calibrated_.*\\.csv$")
    files   <- list.files(file.path("calibrated_data", id), pattern, full.names = TRUE)
    if (!length(files)) return(NULL)
    ds      <- sapply(files, function(f) as.Date(strsplit(basename(f), "_")[[1]][3]))
    latest  <- files[which.max(ds)]
    df      <- read_csv(latest, show_col_types = FALSE)
    df$DATE <- parse_datetime(df$DATE)
    
    # cast numeric cols
    num_cols <- intersect(c("AQHI","CO","NO","NO2","O3","CO2","PM1.0","PM2.5","PM10"), names(df))
    for (c in num_cols) df[[c]] <- suppressWarnings(as.numeric(df[[c]]))
    
    # harmonize Top_AQHI_Contributor
    if (!"Top_AQHI_Contributor" %in% names(df)) df$Top_AQHI_Contributor <- NA_character_
    else df$Top_AQHI_Contributor <- as.character(df$Top_AQHI_Contributor)
    
    rec <- df %>% arrange(desc(DATE)) %>% slice(1)
    rec$sensor_id <- id
    rec
  })
  bind_rows(rows)
}

loadHistoricalData <- function(id) {
  pattern <- paste0("^", id, "_calibrated_.*\\.csv$")
  files   <- list.files(file.path("calibrated_data", id), pattern, full.names = TRUE)
  if (!length(files)) return(NULL)
  ds      <- sapply(files, function(f) as.Date(strsplit(basename(f), "_")[[1]][3]))
  latest  <- files[which.max(ds)]
  df      <- read_csv(latest, show_col_types = FALSE)
  df$DATE <- parse_datetime(df$DATE)
  df %>% filter(DATE >= Sys.time() - 24*3600)
}

# 4. Pollutant metadata
pollutants <- c(CO="ppm", NO="ppb", NO2="ppb", O3="ppb",
                CO2="ppm", `PM2.5`="µg/m³", PM10="µg/m³")

# 5. UI Definition
ui <- fluidPage(
  includeCSS("www/styles.css"),
  navbarPage("Air Quality Dashboard", id = "navbar",
             tabPanel("Home",
                      fluidPage(
                        fluidRow(
                          column(12,
                                 p("This dashboard displays nothing of interest at this point in time."),
                                 div(id = "advisories", style = "background-color:#f8d7da;padding:10px;border-radius:5px;",
                                     strong("Active Air Quality Advisories:"), p("No advisories are currently active.")),
                                 br(),
                                 div(class = "button-container",
                                     actionButton("map_page",  "View Map",      class = "nav-button"),
                                     actionButton("list_page", "Detailed View", class = "nav-button"),
                                     actionButton("info_page", "Info",          class = "nav-button")
                                 )
                          )
                        )
                      )
             ),
             tabPanel("Map",
                      fluidRow(
                        column(12, leafletOutput("airQualityMap", height = "60vh")),
                        column(12,
                               selectInput("sensor_select", "Select Sensor", choices = names(sensor_locations)),
                               textOutput("last_update"), uiOutput("sensor_details")
                        )
                      )
             ),
             tabPanel("Detailed View",
                      fluidPage(
                        sidebarLayout(
                          sidebarPanel(
                            selectInput("d_sensor",   "Sensor:",    choices = names(sensor_locations)),
                            selectInput("d_pollutant","Pollutant:", choices = names(pollutants))
                          ),
                          mainPanel(
                            h4(textOutput("dv_title")),
                            plotOutput("d_plot"),
                            verbatimTextOutput("d_desc")
                          )
                        )
                      )
             ),
             tabPanel("Info",
                      fluidPage(h2("Information Page - Coming Soon"))
             )
  )
)

# 6. Server Logic
server <- function(input, output, session) {
  # navigation
  observeEvent(input$map_page,  updateNavbarPage(session, "navbar", "Map"))
  observeEvent(input$list_page, updateNavbarPage(session, "navbar", "Detailed View"))
  observeEvent(input$info_page, updateNavbarPage(session, "navbar", "Info"))
  
  # map data
  sensor_data <- reactive(loadCalibratedData(names(sensor_locations)))
  
  output$airQualityMap <- renderLeaflet({
    df <- sensor_data(); m <- leaflet() %>% addTiles()
    if (nrow(df)) {
      df <- df %>% mutate(
        col = vapply(AQHI, getAQHIColor, character(1)),
        popup = paste0("<b>", sensor_id, "</b><br>AQHI: ", round(AQHI,1))
      )
      for (i in seq_len(nrow(df))) {
        loc <- sensor_locations[[df$sensor_id[i]]]
        m <- addCircleMarkers(m, lng = loc$lng, lat = loc$lat,
                              color = df$col[i], radius=8, fillOpacity=0.8,
                              popup = df$popup[i], layerId = df$sensor_id[i])
      }
    }
    m
  })
  
  observeEvent(input$sensor_select, {
    loc <- sensor_locations[[input$sensor_select]]
    leafletProxy("airQualityMap") %>% setView(loc$lng, loc$lat, zoom=12)
  })
  
  output$last_update <- renderText({
    df <- sensor_data()
    if (!nrow(df)) return("No data")
    paste("Last updated:", format(max(df$DATE), "%Y-%m-%d %H:%M"))
  })
  
  output$sensor_details <- renderUI({
    req(input$sensor_select)
    row <- sensor_data() %>% filter(sensor_id == input$sensor_select)
    wellPanel(
      h4(paste0("Sensor ", input$sensor_select)),
      p(getAQHIDescription(row$AQHI))
    )
  })
  
  # Detailed View
  output$dv_title <- renderText({
    paste(input$d_pollutant, "– Sensor", input$d_sensor)
  })
  
  output$d_desc <- renderText({
    df <- loadHistoricalData(input$d_sensor)
    req(nrow(df))
    desc <- getAQHIDescription(df$AQHI[nrow(df)])
    paste("Current AQHI:", round(df$AQHI[nrow(df)],1), "(", desc, ")")
  })
  
  output$d_plot <- renderPlot({
    req(input$d_sensor, input$d_pollutant)
    df <- loadHistoricalData(input$d_sensor)
    validate(need(nrow(df)>0, "No data past 24h"))
    df$DATE <- parse_datetime(df$DATE)
    df$val  <- as.numeric(df[[input$d_pollutant]])
    validate(need(!all(is.na(df$val)), "No pollutant data"))
    
    ggplot(df, aes(x=DATE, y=val)) +
      geom_line(linewidth=1.2) +
      labs(x="Time", y=pollutants[input$d_pollutant]) +
      theme_minimal()
  })
}

# 7. Launch app
shinyApp(ui, server)