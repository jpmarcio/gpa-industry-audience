pipeline {
  agent any
  stages {
    stage('Cloning Git') {
      steps {
        git url: 'https://github.com/jpmarcio/gpa-industry-audience.git/'
      }
    }
    stage('Building image') {
      steps{
        script {
          sh "docker build -t gpa-industry-audience ."
          sh "docker run -p 9999:9999 --name gpa-industry-audience -d gpa-industry-audience "
        }
      }
    }