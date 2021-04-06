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
              }
           }
        }
        stage('Final Stage'){
            steps{
                echo "Deu tudo certo!!!"
            }
        }
    }
}
