package com.clickhouse.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Generates the class {@code <Enum>Descriptions} in the package of each top-level enum that has constants annotated
 * with {@code com.clickhouse.client.api.Description}. The static method {@code getDescriptions()} of the generated
 * class returns an unmodifiable map from each constant of the enum to its description.
 * <p>
 * All constants of such an enum must have a description that is not blank. A constant without a description, a
 * blank description, or the annotation on an element that is not a constant of a top-level enum fails the
 * compilation.
 */
@SupportedAnnotationTypes(DescriptionProcessor.DESCRIPTION_ANNOTATION)
public class DescriptionProcessor extends AbstractProcessor {

    static final String DESCRIPTION_ANNOTATION = "com.clickhouse.client.api.Description";

    static final String CLASS_NAME_SUFFIX = "Descriptions";

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            Set<TypeElement> enums = new LinkedHashSet<>();
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (element.getKind() != ElementKind.ENUM_CONSTANT) {
                    error(element, "@Description is allowed only on enum constants");
                } else if (((TypeElement) element.getEnclosingElement()).getNestingKind() != NestingKind.TOP_LEVEL) {
                    error(element, "@Description is allowed only on constants of top-level enums");
                } else {
                    enums.add((TypeElement) element.getEnclosingElement());
                }
            }
            for (TypeElement enumType : enums) {
                Map<String, String> descriptions = readDescriptions(enumType);
                if (descriptions != null) {
                    writeDescriptionsClass(enumType, descriptions);
                }
            }
        }
        return true;
    }

    /**
     * Returns the descriptions of all constants of the enum by constant name, in declaration order, or {@code null}
     * when a constant has no description or a blank description.
     */
    private Map<String, String> readDescriptions(TypeElement enumType) {
        Map<String, String> descriptions = new LinkedHashMap<>();
        boolean valid = true;
        for (Element constant : enumType.getEnclosedElements()) {
            if (constant.getKind() != ElementKind.ENUM_CONSTANT) {
                continue;
            }
            String name = enumType.getSimpleName() + "." + constant.getSimpleName();
            String description = descriptionOf(constant);
            if (description == null) {
                error(constant, "Enum constant " + name + " has no @Description");
                valid = false;
            } else if (description.trim().isEmpty()) {
                error(constant, "Enum constant " + name + " has a blank @Description");
                valid = false;
            } else {
                descriptions.put(constant.getSimpleName().toString(), description);
            }
        }
        return valid ? descriptions : null;
    }

    private static String descriptionOf(Element constant) {
        for (AnnotationMirror mirror : constant.getAnnotationMirrors()) {
            TypeElement type = (TypeElement) mirror.getAnnotationType().asElement();
            if (!type.getQualifiedName().contentEquals(DESCRIPTION_ANNOTATION)) {
                continue;
            }
            for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> value
                    : mirror.getElementValues().entrySet()) {
                if (value.getKey().getSimpleName().contentEquals("value")) {
                    return String.valueOf(value.getValue().getValue());
                }
            }
        }
        return null;
    }

    private void writeDescriptionsClass(TypeElement enumType, Map<String, String> descriptions) {
        String packageName = processingEnv.getElementUtils().getPackageOf(enumType).getQualifiedName().toString();
        String enumName = enumType.getSimpleName().toString();
        String className = enumName + CLASS_NAME_SUFFIX;
        String visibility = enumType.getModifiers().contains(Modifier.PUBLIC) ? "public " : "";

        StringBuilder src = new StringBuilder();
        if (!packageName.isEmpty()) {
            src.append("package ").append(packageName).append(";\n\n");
        }
        src.append("/**\n")
            .append(" * Descriptions of the constants of {@link ").append(enumName)
            .append("}, from their {@code @Description} annotations.\n")
            .append(" * Generated by {@code ").append(DescriptionProcessor.class.getName())
            .append("}. Do not edit.\n")
            .append(" */\n")
            .append("@SuppressWarnings(\"deprecation\")\n")
            .append(visibility).append("final class ").append(className).append(" {\n\n")
            .append("    private static final java.util.Map<").append(enumName).append(", String> DESCRIPTIONS;\n\n")
            .append("    static {\n")
            .append("        java.util.Map<").append(enumName).append(", String> descriptions = new java.util.EnumMap<>(")
            .append(enumName).append(".class);\n");
        for (Map.Entry<String, String> description : descriptions.entrySet()) {
            src.append("        descriptions.put(").append(enumName).append('.').append(description.getKey())
                .append(", ").append(toJavaLiteral(description.getValue())).append(");\n");
        }
        src.append("        DESCRIPTIONS = java.util.Collections.unmodifiableMap(descriptions);\n")
            .append("    }\n\n")
            .append("    private ").append(className).append("() {\n")
            .append("    }\n\n")
            .append("    /**\n")
            .append("     * Returns an unmodifiable map from each constant of {@link ").append(enumName)
            .append("} to its description.\n")
            .append("     *\n")
            .append("     * @return the descriptions, in the order of the constants\n")
            .append("     */\n")
            .append("    public static java.util.Map<").append(enumName).append(", String> getDescriptions() {\n")
            .append("        return DESCRIPTIONS;\n")
            .append("    }\n")
            .append("}\n");

        String qualifiedName = packageName.isEmpty() ? className : packageName + "." + className;
        try (Writer writer = processingEnv.getFiler().createSourceFile(qualifiedName, enumType).openWriter()) {
            writer.write(src.toString());
        } catch (IOException e) {
            error(enumType, "Failed to write " + qualifiedName + ": " + e.getMessage());
        }
    }

    /**
     * Returns the Java string literal of the value. Uses octal escapes for control characters and Unicode escapes
     * for characters that are not ASCII: a Unicode escape of a line terminator, a quote or a backslash would end or
     * break the literal, because the compiler translates Unicode escapes before it reads the literal.
     */
    static String toJavaLiteral(String value) {
        StringBuilder literal = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                literal.append('\\').append(c);
            } else if (c < 0x20 || c == 0x7f) {
                literal.append(String.format("\\%03o", (int) c));
            } else if (c > 0x7f) {
                literal.append(String.format("\\u%04x", (int) c));
            } else {
                literal.append(c);
            }
        }
        return literal.append('"').toString();
    }

    private void error(Element element, String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
    }
}
