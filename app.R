library(shiny)
library(leaflet)

ui <- navbarPage(
  "Air Quality Dashboard",
  tabPanel(
    "Home",
    fluidPage(
      titlePanel("Welcome to the Air Quality Dashboard"),
      fluidRow(
        column(12, 
               p("This dashboard displays real-time air pollution data."),
               div(id = "advisories", style = "background-color: #f8d7da; padding: 10px; border-radius: 5px;",
                   strong("Active Air Quality Advisories:"),
                   p("None at the moment.")
               ),
               br(),
               fluidRow(
                 column(4, actionButton("map_page", "View Map", class = "nav-button")),
                 column(4, actionButton("list_page", "View List", class = "nav-button")),
                 column(4, actionButton("info_page", "Info", class = "nav-button"))
               )
        )
      )
    )
  ),
  tabPanel(
    "Map",
    fluidPage(
      leafletOutput("airQualityMap")
    )
  )
)

server <- function(input, output, session) {
  # Navigation
  observeEvent(input$map_page, {
    updateNavbarPage(session, "Air Quality Dashboard", selected = "Map")
  })
  
  # Placeholder map
  output$airQualityMap <- renderLeaflet({
    leaflet() %>% 
      addTiles() %>% 
      setView(lng = -123.3656, lat = 48.4284, zoom = 10)
  })
}

shinyApp(ui, server)
