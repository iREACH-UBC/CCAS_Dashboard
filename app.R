server <- function(input, output, session) {

  observeEvent(input$map_page, {
    updateNavbarPage(session, "navbar", selected = "Map")
  })
  observeEvent(input$list_page, {
    updateNavbarPage(session, "navbar", selected = "List")
  })
  observeEvent(input$info_page, {
    updateNavbarPage(session, "navbar", selected = "Info")
  })
  
  # Use reactivePoll to check the last modification time of any CSV file in calibrated_data.
  sensor_data <- reactivePoll(60000, session,
    # This check function returns the latest modification time among all CSV files.
    checkFunc = function() {
      files <- list.files("calibrated_data", pattern = "\\.csv$", full.names = TRUE)
      if(length(files) == 0) return(0)
      max(file.info(files)$mtime)
    },
    # When the checkFunc value changes, read the calibrated data.
    valueFunc = function() {
      loadCalibratedData(names(sensor_locations))
    }
  )
  
  output$airQualityMap <- renderLeaflet({
    m <- leaflet() %>% addTiles()
    df <- sensor_data()
    if(nrow(df) > 0) {
      df <- df %>%
        mutate(
          marker_color = sapply(AQI, getAQIColor),
          popup_text = paste0(
            "<b>Sensor: ", sensor_id, "</b><br>",
            "AQI: ", round(AQI, 1), "<br>",
            "CO: ", CO, "<br>",
            "NO: ", NO, "<br>",
            "NO₂: ", NO2, "<br>",
            "O₃: ", O3, "<br>",
            "CO₂: ", CO2, "<br>",
            "PM1.0: ", `PM1.0`, "<br>",
            "PM2.5: ", `PM2.5`, "<br>",
            "PM10: ", PM10, "<br>",
            "Temp: ", T, " °C<br>",
            "RH: ", RH, " %"
          )
        )
      
      for(i in 1:nrow(df)) {
        sensor_id <- df$sensor_id[i]
        loc <- sensor_locations[[sensor_id]]
        if(!is.null(loc)) {
          m <- m %>% addCircleMarkers(
            lng = loc$lng,
            lat = loc$lat,
            color = df$marker_color[i],
            radius = 8,
            fillOpacity = 0.8,
            popup = df$popup_text[i]
          )
        }
      }
    }
    m
  })
}
