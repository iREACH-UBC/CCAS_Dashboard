library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)
library(ggplot2)

# ----------------------------------------------------------------------------
# 1. Sensor location definitions
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

# ----------------------------------------------------------------------------
# 2. Helpers
# ----------------------------------------------------------------------------
parse_datetime <- function(x) {
  x_chr <- as.character(x)
  dt <- suppressWarnings(ymd_hms(x_chr, tz = "America/Vancouver"))
  if (all(is.na(dt))) dt <- suppressWarnings(ymd_hm(x_chr, tz = "America/Vancouver"))
  if (all(is.na(dt))) dt <- suppressWarnings(ymd(x_chr,    tz = "America/Vancouver"))
  as.POSIXct(dt, tz = "America/Vancouver")
}

getAQHIColor <- function(aqhi) {
  a <- suppressWarnings(as.numeric(aqhi))
  if (is.na(a))      "gray"
  else if (a <= 1)   "#67c1f1"
  else if (a <= 2)   "#4e95c7"
  else if (a <= 3)   "#396798"
  else if (a <= 4)   "#e7eb38"
  else if (a <= 5)   "#f1cb2e"
  else if (a <= 6)   "#e79647"
  else if (a <= 7)   "#dd6869"
  else if (a <= 8)   "#d82732"
  else if (a <= 9)   "#bf2733"
  else if (a <= 10)  "#8b2328"
  else               "#5a161b"
}

getAQHIDescription <- function(aqhi) {
  a <- suppressWarnings(as.numeric(aqhi))
  if (is.na(a))      "No data available"
  else if (a <= 3)   "Low health risk"
  else if (a <= 6)   "Moderate health risk"
  else if (a <= 10)  "High health risk"
  else               "Very high health risk"
}

# ----------------------------------------------------------------------------
# 3. Data loaders
# ----------------------------------------------------------------------------
loadCalibratedData <- function(sensor_ids) {
  rows <- lapply(sensor_ids, function(id) {
    pattern <- paste0("^", id, "_calibrated_.*\\.csv$")
    files   <- list.files(file.path("calibrated_data", id),
                          pattern, full.names = TRUE)
    if (!length(files)) return(NULL)
    dates   <- sapply(files, function(f)
      as.Date(strsplit(basename(f), "_")[[1]][3]))
    latest  <- files[which.max(dates)]
    df      <- read_csv(latest, show_col_types = FALSE)
    df$DATE <- parse_datetime(df$DATE)
    
    num_cols <- intersect(
      c("AQHI","CO","NO","NO2","O3","CO2","PM1.0","PM2.5","PM10"),
      names(df)
    )
    for (c in num_cols) df[[c]] <- suppressWarnings(as.numeric(df[[c]]))
    
    if (!"Top_AQHI_Contributor" %in% names(df))
      df$Top_AQHI_Contributor <- NA_character_
    else
      df$Top_AQHI_Contributor <- as.character(df$Top_AQHI_Contributor)
    
    rec <- df %>% arrange(desc(DATE)) %>% slice(1)
    rec$sensor_id <- id
    rec
  })
  bind_rows(rows)
}

loadHistoricalData <- function(id) {
  pattern <- paste0("^", id, "_calibrated_.*\\.csv$")
  files   <- list.files(file.path("calibrated_data", id),
                        pattern, full.names = TRUE)
  if (!length(files)) return(NULL)
  dates   <- sapply(files, function(f)
    as.Date(strsplit(basename(f), "_")[[1]][3]))
  latest  <- files[which.max(dates)]
  df      <- read_csv(latest, show_col_types = FALSE)
  df$DATE <- parse_datetime(df$DATE)
  df %>% filter(DATE >= Sys.time() - 24*3600)
}

# ----------------------------------------------------------------------------
# 4. Pollutant metadata
# ----------------------------------------------------------------------------
pollutants <- c(
  CO    = "ppm",
  NO    = "ppb",
  NO2   = "ppb",
  O3    = "ppb",
  CO2   = "ppm",
  `PM2.5` = "µg/m³",
  PM10  = "µg/m³"
)

# ----------------------------------------------------------------------------
# 5. UI Definition
# ----------------------------------------------------------------------------
ui <- fluidPage(
  includeCSS("www/styles.css"),
  navbarPage("Air Quality Dashboard", id = "navbar",
             
             tabPanel("Home", fluidPage(
               fluidRow(column(12,
                               p("This dashboard displays nothing of interest at this point in time."),
                               div(id = "advisories",
                                   style="background-color:#f8d7da;padding:10px;border-radius:5px;",
                                   strong("Active Air Quality Advisories:"), p("No advisories are currently active.")),
                               br(),
                               div(class="button-container",
                                   actionButton("map_page","View Map",      class="nav-button"),
                                   actionButton("list_page","Detailed View",class="nav-button"),
                                   actionButton("info_page","Info",         class="nav-button"))
               ))
             )),
             
             tabPanel("Map", fluidRow(
               column(12, leafletOutput("airQualityMap", height="60vh")),
               column(12,
                      selectInput("sensor_select","Select Sensor",choices=names(sensor_locations)),
                      textOutput("last_update"),
                      uiOutput("sensor_details")
               )
             )),
             
             tabPanel("Detailed View", fluidPage(
               sidebarLayout(
                 sidebarPanel(
                   selectInput("d_sensor",    "Sensor:",    choices=names(sensor_locations)),
                   selectInput("d_pollutant", "Pollutant:", choices=names(pollutants))
                 ),
                 mainPanel(
                   h4(textOutput("dv_title")),
                   plotOutput("d_plot"),
                   verbatimTextOutput("d_desc")
                 )
               )
             )),
             
             tabPanel("Info", fluidPage(
               h2("Information Page - Coming Soon")
             ))
             
  )
)

# ----------------------------------------------------------------------------
# 6. Server Logic
# ----------------------------------------------------------------------------
server <- function(input, output, session) {
  
  # Risk‐band definitions for pollutant
  pollutant_breaks <- list(
    CO     = c(0,   4.4,    9.4,   12.4,   15.4, 30.4, Inf),
    NO     = c(0,  50,  150,  300, 650, 1250, Inf),
    NO2    = c(0,  53,  100,  360, 649, 1249, Inf),
    O3     = c(0,  54,  70,  164, 204, 404, 604, Inf),
    CO2    = c(0, 600, 1000, 1500, 2000, 3000, Inf),
    `PM2.5`= c(0,  9.0,  35.4, 55.4, 125.4, 225.4, Inf),
    PM10   = c(0,  54,   154,  254, 354, 424, Inf)
  )
  pollutant_cols <- c("lightblue","khaki","orange","red", "purple", "maroon")
  
  # Navigation buttons
  observeEvent(input$map_page,  updateNavbarPage(session,"navbar","Map"))
  observeEvent(input$list_page, updateNavbarPage(session,"navbar","Detailed View"))
  observeEvent(input$info_page, updateNavbarPage(session,"navbar","Info"))
  
  # Map: load & render
  sensor_data <- reactive(loadCalibratedData(names(sensor_locations)))
  output$airQualityMap <- renderLeaflet({
    df <- sensor_data(); m <- leaflet() %>% addTiles()
    if (nrow(df)) {
      df <- df %>% mutate(
        col   = vapply(AQHI, getAQHIColor, character(1)),
        popup = paste0("<b>", sensor_id, "</b><br>AQHI: ", round(AQHI,1))
      )
      for (i in seq_len(nrow(df))) {
        loc <- sensor_locations[[df$sensor_id[i]]]
        m <- addCircleMarkers(m,
                              lng=loc$lng, lat=loc$lat,
                              color=df$col[i], radius=8, fillOpacity=0.8,
                              popup=df$popup[i], layerId=df$sensor_id[i]
        )
      }
    }
    m
  })
  observeEvent(input$sensor_select,{
    loc <- sensor_locations[[input$sensor_select]]
    leafletProxy("airQualityMap") %>% setView(loc$lng, loc$lat, zoom=12)
  })
  output$last_update <- renderText({
    df <- sensor_data()
    if (!nrow(df)) return("No data")
    paste("Last updated:", format(max(df$DATE),"%Y-%m-%d %H:%M"))
  })
  output$sensor_details <- renderUI({
    req(input$sensor_select)
    row <- sensor_data() %>% filter(sensor_id==input$sensor_select)
    wellPanel(h4(paste0("Sensor ", input$sensor_select)),
              p(getAQHIDescription(row$AQHI)))
  })
  
  # Detailed View: title & description
  output$dv_title <- renderText({
    paste(input$d_pollutant, "– Sensor", input$d_sensor)
  })
  output$d_desc <- renderText({
    df <- loadHistoricalData(input$d_sensor); req(nrow(df))
    a  <- round(df$AQHI[nrow(df)],1)
    paste("Current AQHI:", a, "(", getAQHIDescription(a), ")")
  })
  
  # Detailed View: single‐series pollutant plot with static risk bands
  output$d_plot <- renderPlot({
    req(input$d_sensor, input$d_pollutant)
    df <- loadHistoricalData(input$d_sensor)
    validate(need(nrow(df)>0, "No data past 24 h"))
    df$DATE <- parse_datetime(df$DATE)
    df$val  <- suppressWarnings(as.numeric(df[[input$d_pollutant]]))
    validate(need(!all(is.na(df$val)), "No pollutant data"))
    
    brks <- pollutant_breaks[[input$d_pollutant]]
    cols <- pollutant_cols
    
    ggplot(df, aes(x=DATE, y=val)) +
      geom_rect(xmin=-Inf, xmax=Inf, ymin=brks[1], ymax=brks[2],
                fill=cols[1], alpha=.2, inherit.aes=FALSE) +
      geom_rect(xmin=-Inf, xmax=Inf, ymin=brks[2], ymax=brks[3],
                fill=cols[2], alpha=.2, inherit.aes=FALSE) +
      geom_rect(xmin=-Inf, xmax=Inf, ymin=brks[3], ymax=brks[4],
                fill=cols[3], alpha=.2, inherit.aes=FALSE) +
      geom_rect(xmin=-Inf, xmax=Inf, ymin=brks[4], ymax=brks[5],
                fill=cols[4], alpha=.2, inherit.aes=FALSE) +
      geom_line(linewidth=1.2) +
      labs(x="Time", y=pollutants[input$d_pollutant]) +
      theme_minimal()
  })
}

# ----------------------------------------------------------------------------
# 7. Launch app
# ----------------------------------------------------------------------------
shinyApp(ui, server)