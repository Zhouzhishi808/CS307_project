package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/users")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/register")
    public Object register(@RequestBody RegisterUserReq req) {
        long userId = userService.register(req);
        return userId == -1 ? "注册失败" : Map.of("userId", userId);
    }

    @PostMapping("/login")
    public Object login(@RequestBody AuthInfo auth) {
        long userId = userService.login(auth);
        return userId == -1 ? "登录失败" : Map.of("userId", userId);
    }

    @DeleteMapping("/{userId}")
    public Object deleteAccount(@RequestBody AuthInfo auth, @PathVariable long userId) {
        boolean result = userService.deleteAccount(auth, userId);
        return Map.of("deleted", result);
    }

    @PostMapping("/{followeeId}/follow")
    public Object follow(@RequestBody AuthInfo auth, @PathVariable long followeeId) {
        boolean result = userService.follow(auth, followeeId);
        return Map.of("following", result);
    }

    @GetMapping("/{userId}")
    public UserRecord getById(@PathVariable long userId) {
        return userService.getById(userId);
    }

    @PutMapping("/profile")
    public String updateProfile(@RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        String gender = (String) request.get("gender");
        Integer age = (Integer) request.get("age");
        
        userService.updateProfile(auth, gender, age);
        return "更新成功";
    }

    @GetMapping("/feed")
    public PageResult<FeedItem> feed(@RequestParam long authorId, 
                                    @RequestParam(required = false) String password,
                                    @RequestParam(defaultValue = "1") int page,
                                    @RequestParam(defaultValue = "10") int size,
                                    @RequestParam(required = false) String category) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(authorId);
        auth.setPassword(password);
        return userService.feed(auth, page, size, category);
    }

    @GetMapping("/highest-follow-ratio")
    public Map<String, Object> getHighestFollowRatio() {
        return userService.getUserWithHighestFollowRatio();
    }
}