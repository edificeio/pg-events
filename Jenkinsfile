pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
        sh 'mvn clean package'
      }
    }
    stage('Publish') {
      steps {
        sh 'mvn deploy'
        sh '''
          export jarFile=`ls target/pg-events-*.jar`
          export jarVersion=`echo $jarFile | sed 's|target/pg-events-||' | sed 's/.jar//'`
        case "$jarVersion" in
          *SNAPSHOT) export nexusRepository='snapshots' ;;
          *)         export nexusRepository='releases' ;;
          esac
            mvn deploy:deploy-file -DgroupId=com.opendigitaleducation -DartifactId=pg-events -Dversion=$jarVersion -Dpackaging=jar -Dfile=$jarFile -DrepositoryId=ode-$nexusRepository -Durl=https://maven.opendigitaleducation.com/nexus/content/repositories/$nexusRepository/
        '''
      }
    }
  }
}

