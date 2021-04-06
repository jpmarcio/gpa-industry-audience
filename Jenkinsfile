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
//                  sh "docker build -t re037737/gpa-industry-audience ."
              }
           }
        }
        stage('Deploy image') {
           steps{
              script {
                 sh "ls -l"
                 sh "cd api-dh-v2"
                 sh "sh meudesconto_api.sh status dev"
//                  sh "sh api-dh-v2/meudesconto_api.sh status dev"
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
