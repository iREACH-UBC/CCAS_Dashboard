# app.R
library(shiny)
library(leaflet)
library(dplyr)

# Load sample sensor data (Replace with actual data source)
sensor_data <- data.frame(
  longitude = c(-123.3656, -123.2609, -123.1207),
  latitude = c(48.4284, 49.2827, 49.2463),
  AQI = c(50, 120, 200) # Example AQI values
)

# Function to determine marker color based on AQI
getAQIColor <- function(aqi) {
  if (aqi <= 50) {
    return("green")
  } else if (aqi <= 100) {
    return("yellow")
  } else if (aqi <= 150) {
    return("orange")
  } else if (aqi <= 200) {
    return("red")
  } else if (aqi <= 300) {
    return("purple")
  } else {
    return("maroon")
  }
}

ui <- fluidPage(
  tags$head(tags$style(HTML("body { margin: 0; padding: 0; overflow-x: hidden; }
                            .title-bar { width: 100vw; background-color: #002145; color: white; padding: 20px; display: flex; justify-content: space-between; align-items: center; box-sizing: border-box; margin: 0; }
                            .title-left { font-size: 20px; font-weight: bold; margin-left: 0; padding-left: 10px; }
                            .title-center { font-size: 24px; font-weight: bold; text-align: center; flex-grow: 1; }
                            .title-right { display: flex; flex-direction: column; align-items: center; margin-right: 10px; }
                            .nav-button { width: 30vw; height: 30vw; max-width: 300px; max-height: 300px; background-size: cover; background-position: center; border: none; cursor: pointer; font-size: 24px; text-align: center; color: white; font-weight: bold; opacity: 0.8; display: flex; align-items: center; justify-content: center; text-shadow: 2px 2px 4px black; }
                            .nav-button:hover { opacity: 1; text-shadow: none; }
                            .button-container { display: flex; justify-content: space-around; margin-top: 20px; }
                            #airQualityMap { width: 75vw; height: 75vh; max-width: 800px; max-height: 600px; margin-right: 5vw; }"))),
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
    tabPanel(
      "Home",
      fluidPage(
        fluidRow(
          column(12, 
                 p("This dashboard displays nothing of importance."),
                 div(id = "advisories", style = "background-color: #f8d7da; padding: 10px; border-radius: 5px;",
                     strong("Active Air Quality Advisories:"),
                     p("None at the moment (I actually checked this, most recent one ended 3 days ago).")
                 ),
                 br(),
                 div(class = "button-container",
                     actionButton("map_page", "View Map", class = "nav-button", style = "background-image: url('map.png');"),
                     actionButton("list_page", "View List", class = "nav-button", style = "background-image: url('placeholder-image.jpg');"),
                     actionButton("info_page", "Info", class = "nav-button", style = "background-image: url('placeholder-image.jpg');"),
                 )
          )
        )
      )
    ),
    tabPanel(
      "Map",
      fluidPage(
        leafletOutput("airQualityMap", width = "75vw", height = "75vh")
      )
    ),
    tabPanel(
      "List",
      fluidPage(
        h2("List View - Coming Soon")
      )
    ),
    tabPanel(
      "Info",
      fluidPage(
        h2("Information Page - Coming Soon")
      )
    )
  )
)

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
  
  output$airQualityMap <- renderLeaflet({
    leaflet(sensor_data) %>% 
      addTiles() %>% 
      addCircleMarkers(
        lng = ~longitude,
        lat = ~latitude,
        color = ~sapply(AQI, getAQIColor),
        radius = 8,
        stroke = FALSE,
        fillOpacity = 0.8,
        label = ~paste("AQI:", AQI)
      )
  })
}

shinyApp(ui, server)
