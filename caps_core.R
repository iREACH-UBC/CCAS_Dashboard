CAPS_PR_Train = function(v_target,m_data,model_settings=c(1)) {
  # This function trains polynomial regression models
  
  # INPUTS
  
  # v_target: vector of target data for training
  
  # m_data: matrix of input data to fit to target. First dimension should match length of v_target
  
  # model_settings[1]: The degree of the polynomial to be applied
  
  # OUTPUT
  
  # model_object: vector of coefficients of the polynomial regression model
  
  ## Setup
  # Augment data matrix
  m_data_new = matrix(1,dim(m_data)[1],(dim(m_data)[2]+1))
  m_data_new[,2:(dim(m_data)[2]+1)] = m_data
  m_data = m_data_new
  
  # create factor matrix
  n_order = model_settings[1]
  y = as.matrix(v_target)
  
  m_combinations = combinations(dim(m_data)[2],n_order,repeats.allowed = TRUE)
  
  X = matrix(NA,dim(m_data)[1],dim(m_combinations)[1]) 
  
  for (i_combin in 1:(dim(m_combinations)[1])) {
    if (dim(m_combinations)[2] == 1) {
      X[,i_combin] = m_data[,m_combinations[i_combin,]]
    } else {
      X[,i_combin] = apply(m_data[,m_combinations[i_combin,]],-2,prod)
    }
  }
  
  ## Train Model
  # n_tolerance = (10 ^ -10)
  # if (abs(det(t(X) %*% X)) > n_tolerance) {parameters = solve((t(X) %*% X),(t(X) %*%y))} else {parameters = NaN}
  
  linear_model = lm(y ~ X[,2:(dim(X)[2])])
  parameters = linear_model$coefficients
  summary_lm = summary(linear_model) # added by Davi on Dec. 2023
  names(parameters) = 1:dim(X)[2]
  parameters[is.na(parameters)] = 0
  
  ## Output
  model_object = list(parameters, summary_lm) # modified by Davi on Dec. 2023
  return(model_object)
}
CAPS_PR_Apply = function(model_object,m_data) {
  # This function applies polynomial regression models
  
  # INPUTS
  
  # model_object: vector of coefficients of the polynomial regression model.
  
  # m_data: matrix of input data to fit to target.
  
  # OUTPUT
  
  # v_data: vector of fitted data.
  
  if ((is_empty(m_data)) | (is_empty(model_object))) {
    v_data = as.matrix(rep(NaN,dim(m_data)[1]))
  } else {
    
    ## Setup
    # Augment data matrix
    m_data_new = matrix(1,dim(m_data)[1],(dim(m_data)[2]+1))
    m_data_new[,2:(dim(m_data)[2]+1)] = m_data
    m_data = m_data_new
    
    # determine model order
    parameters = as.matrix(model_object)
    n_parameters = length(parameters)
    
    n_order = 0;
    if (n_parameters == 1) {
      n_order = 0
    } else if (n_parameters == dim(m_data)[2]) {
      n_order = 1
    } else if (n_parameters == sum(1:(dim(m_data)[2]))) {
      n_order = 2
    } else {
      i_order = 3
      while (n_order == 0) {
        m_combinations = combinations(dim(m_data)[2],i_order,repeats.allowed = TRUE)
        n_combinations = dim(m_combinations)[1]
        if (n_parameters == n_combinations) {
          n_order = i_order
        } else {
          i_order = i_order + 1
        }
      }
    }
    
    # create factor matrix
    m_combinations = combinations(dim(m_data)[2],n_order,repeats.allowed = TRUE)
    
    X = matrix(NA,dim(m_data)[1],dim(m_combinations)[1]) 
    
    for (i_combin in 1:(dim(m_combinations)[1])) {
      if (dim(m_combinations)[2] == 1) {
        X[,i_combin] = m_data[,m_combinations[i_combin,]]
      } else {
        X[,i_combin] = apply(m_data[,m_combinations[i_combin,]],-2,prod)
      }
    }
    
    ## Remove columns with zero coefficient
    id_zero_columns = which(parameters == 0)
    if (!is_empty(id_zero_columns)) {
      X = X[,-id_zero_columns]
      parameters = parameters[-id_zero_columns]
      parameters = as.matrix(parameters)
    }
    
    ## Apply Model
    v_data = X %*% parameters
    
    v_data[is.na(v_data)] = NaN
    
  }
  
  ## Output
  return(v_data)
}
CAPS_RF_Train = function(v_target,m_data,model_settings=c(100)) {
  # The function trains a random forest model
  
  # INPUTS:
  
  # v_target: vector of target data for training
  
  # m_data: matrix of input data to fit to target. First dimension should match length of v_target
  
  # model_settings[1]: The number of trees to be used in the random forest model.
  
  # OUTPUT:
  
  # model_object: The R random forest model to be applied
  
  ## Setup
  warnstate_save = getOption('warn')
  options(warn = -1)
  
  library(randomForest)
  
  if (is_empty(model_settings)) {model_settings = c(100)}
  
  # rename input types for consistency
  c_input_names = c()
  for (i_input in 1:dim(m_data)[2]) {
    c_input_names = c(c_input_names,paste('input',i_input,sep=''))
  }
  colnames(m_data) = c_input_names
  
  ## Train Model
  f_data = as.data.frame(list(v_target,m_data))
  colnames(f_data) = c('target',c_input_names)
  f_data = f_data[complete.cases(f_data),]
  
  ctrl = trainControl(method = "cv",number = 5,verboseIter = TRUE)
  
  if (!is_empty(f_data)) {
    set.seed(1)
    model_object = train(target ~ .,data = f_data,method = "rf",ntrees = model_settings[1],trControl = ctrl)
  } else {
    model_object = NA
  }
  
  ## Output
  detach("package:randomForest",unload = TRUE)
  options(warn = warnstate_save)
  
  return(model_object)
  
}
CAPS_RF_Apply = function(model_object,m_data) {
  # The function applies a random forest model
  
  # INPUTS:
  
  # model_object: The R random forest model to be applied
  
  # m_data: matrix of input data to processed with the model.
  
  # OUTPUT:
  
  # v_data: output of model application. Length matches first dimension of m_data
  
  ## Setup
  warnstate_save = getOption('warn')
  options(warn = -1)
  
  library(randomForest)
  v_data = as.matrix(rep(NaN,dim(m_data)[1]))
  
  ## Apply Model
  if ((!is_empty(model_object)) & (!is_empty(m_data))) {
    # rename input types for consistency
    c_input_names = c()
    for (i_input in 1:dim(m_data)[2]) {
      c_input_names = c(c_input_names,paste('input',i_input,sep=''))
    }
    
    f_data = as.data.frame(m_data)
    colnames(f_data) = c_input_names
    
    id_complete_cases = complete.cases(f_data)
    if (!is_empty(which(id_complete_cases))) {
      v_data[id_complete_cases,] = predict(model_object,newdata = f_data)
    }
  }
  
  ## Output
  detach("package:randomForest",unload = TRUE)
  options(warn = warnstate_save)
  
  v_data = as.matrix(v_data)
  return(v_data)
  
}
CAPS_Hybrid_Train = function(v_target,m_data,model_settings=c(100,0.1,0.95,0.2,0.8)) {
  # The function trains a hybrid random forest and linear regression model
  
  # INPUTS:
  
  # v_target: vector of target data for training
  
  # m_data: matrix of input data to fit to target. First dimension should match length of v_target
  
  # model_settings[1]: The number of trees to be used in the random forest model.
  # model_settings[2]: Lower percentile limit of RF model, e.g. 0.1.
  # model_settings[3]: Upper percentile limit of RF model, e.g. 0.9.
  # model_settings[4]: Upper percentile limit of lower LR training data, e.g. 0.2.
  # model_settings[5]: Lower percentile limit of upper LR training data, e.g. 0.8.
  
  # OUTPUT:
  
  # model_object: The R hybrid random forest and linear regression model to be applied
  # model_object[[1]][1]: lower bound for application of RF model
  # model_object[[1]][2]: upper bound for application of RF model
  # model_object[[2]]: RF model object
  # model_object[[3]]: lower LR model object
  # model_object[[4]]: upper LR model object
  
  
  ## Setup
  if (is_empty(model_settings)) {model_settings = c(100,0.1,0.9,0.2,0.8)}
  model_settings[2] = max(0,model_settings[2])
  model_settings[3] = min(1,model_settings[3])
  model_settings[4] = max(0,model_settings[4])
  model_settings[5] = min(1,model_settings[5])
  
  model_object = rep(list(NA),6) # modified from 4 to 6 by Davi on Dec. 2023
  model_object[[1]] = c(-Inf,Inf)
  
  # rename input types for consistency
  c_input_names = c()
  for (i_input in 1:dim(m_data)[2]) {
    c_input_names = c(c_input_names,paste('input',i_input,sep=''))
  }
  colnames(m_data) = c_input_names
  
  ## Designate limits
  n_lower_LR_upper_bound = quantile(v_target,model_settings[2],na.rm = TRUE)
  n_lower_LR_upper_bound_training = quantile(v_target,model_settings[4],na.rm = TRUE)
  
  n_upper_LR_lower_bound = quantile(v_target,model_settings[3],na.rm = TRUE)
  n_upper_LR_lower_bound_training = quantile(v_target,model_settings[5],na.rm = TRUE)
  
  if (model_settings[2] > 0) {model_object[[1]][1] = n_lower_LR_upper_bound}
  if (model_settings[3] < 1) {model_object[[1]][2] = n_upper_LR_lower_bound}
  
  ## RF Train Model
  model_object[[2]] = CAPS_RF_Train(v_target,m_data,model_settings[1])
  
  ## Train Lower LR Model
  if (model_settings[4] > 0) {
    id_training_data = which(v_target <= n_lower_LR_upper_bound_training)
    if (!is_empty(id_training_data)) {
      # modified to get model summary by Davi on Dec. 2023
      model_object[[3]] = CAPS_PR_Train(v_target[id_training_data],m_data[id_training_data,],model_settings = c(1))[[1]]
      model_object[[5]] = CAPS_PR_Train(v_target[id_training_data],m_data[id_training_data,],model_settings = c(1))[[2]]
    }
  }
  
  ## Train Upper LR Model
  if (model_settings[5] < 1) {
    id_training_data = which(v_target >= n_upper_LR_lower_bound_training)
    if (!is_empty(id_training_data)) {
      # modified to get model summary by Davi on Dec. 2023
      model_object[[4]] = CAPS_PR_Train(v_target[id_training_data],m_data[id_training_data,],model_settings = c(1))[[1]]
      model_object[[6]] = CAPS_PR_Train(v_target[id_training_data],m_data[id_training_data,],model_settings = c(1))[[2]]
    }
  }
  
  ## Output
  return(model_object)
  
}
CAPS_Hybrid_Apply = function(model_object,m_data) {
  # The function applies a hybrid random forest and linear regression model
  
  # INPUTS:
  
  # model_object: The R hybrid random forest and linear regression model to be applied
  # model_object[[1]][1]: lower bound for application of RF model
  # model_object[[1]][2]: upper bound for application of RF model
  # model_object[[2]]: RF model object
  # model_object[[3]]: lower LR model object
  # model_object[[4]]: upper LR model object
  
  # m_data: matrix of input data to processed with the model.
  
  # OUTPUT:
  
  # v_data: output of model application. Length matches first dimension of m_data
  
  ## Setup
  v_data = as.matrix(rep(NaN,dim(m_data)[1]))
  
  ## Apply Model
  if ((!is_empty(model_object)) & (!is_empty(m_data))) {
    # Extract model information
    n_RF_lower_bound = model_object[[1]][1]
    n_RF_upper_bound = model_object[[1]][2]
    
    RF_model = model_object[[2]]
    lower_LR_model = model_object[[3]]
    upper_LR_model = model_object[[4]]
    
    
    # rename input types for consistency
    c_input_names = c()
    for (i_input in 1:dim(m_data)[2]) {
      c_input_names = c(c_input_names,paste('input',i_input,sep=''))
    }
    colnames(m_data) = c_input_names
    
    ## Apply RF model
    if (!is_empty(RF_model)) {
      v_data = CAPS_RF_Apply(RF_model,m_data)
    }
    
    ## Apply lower LR model if needed
    id_lower = which(v_data < n_RF_lower_bound)
    if (!is_empty(id_lower)) {
      if (!is_empty(lower_LR_model)) {
        m_lower = m_data[id_lower,]
        dim(m_lower) = c(length(id_lower),dim(m_data)[2])
        v_data[id_lower] = CAPS_PR_Apply(lower_LR_model,m_lower)
      } else {
        v_data[id_lower] = NaN
      }
    }
    
    ## Apply upper LR model if needed
    id_upper = which(v_data > n_RF_upper_bound)
    if (!is_empty(id_upper)) {
      if (!is_empty(upper_LR_model)) {
        m_upper = m_data[id_upper,]
        dim(m_upper) = c(length(id_upper),dim(m_data)[2])
        v_data[id_upper] = CAPS_PR_Apply(upper_LR_model,m_upper)
      } else {
        v_data[id_upper] = NaN
      }
    }
    
  }
  
  ## Output
  v_data = as.matrix(v_data)
  return(v_data)
  
}
