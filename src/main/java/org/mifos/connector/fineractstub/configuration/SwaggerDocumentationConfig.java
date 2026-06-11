package org.mifos.connector.fineractstub.configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// springfox (Docket/ApiInfo beans) was removed: it is incompatible with
// Spring Boot 3 and unmaintained. The OpenAPI bean below is served by
// springdoc-openapi, which replaces it.
@SuppressWarnings("checkstyle:Dynamic")
@jakarta.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2023-08-10T10:13:07.472376795Z[GMT]")
@Configuration
public class SwaggerDocumentationConfig {

    @Bean
    public OpenAPI openApi() {
        return new OpenAPI().info(new Info().title("Simple Savings Deposit API").description("This is a simple API").termsOfService("")
                .version("1.0.0").license(new License().name("Apache 2.0").url("http://www.apache.org/licenses/LICENSE-2.0.html"))
                .contact(new io.swagger.v3.oas.models.info.Contact().email("you@your-company.com")));
    }

}
