package org.shersfy.datahub.fs.executor.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class PageController extends BaseController{
    
    @Value("${version}")
    private String version;
    
    @RequestMapping("/index.html")
    public ModelAndView index2() {
        ModelAndView mv = new ModelAndView("index");
        mv.addObject("version", version);
        return mv;
    }
    
    @RequestMapping("/")
    public ModelAndView index() {
        return new ModelAndView("redirect:/index.html");
    }
    
}
