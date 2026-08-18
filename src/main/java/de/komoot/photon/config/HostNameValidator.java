package de.komoot.photon.config;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class HostNameValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (value.matches(".*[,&/\\\\].*")) {
            throw new ParameterException("Parameter " + name + " must not contain &, /, \\ or commas.");
        }
    }
}