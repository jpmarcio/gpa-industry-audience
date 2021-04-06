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
                 sh "docker build -t "re037737/gpa-industry-audience ."
              }
           }
        }
        stage('Deploy image') {
           steps{
              script {
                 sh "ls -l"
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
